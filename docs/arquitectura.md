# Arquitectura — SD Calendario

**Versión 3** · 2026-11-02 · **diseño congelado** tras la cuarta ronda de revisión.

> Proceso: toda revisión debe citar la versión y el hash del archivo
> (`git hash-object docs/arquitectura.md`). Sin eso, la revisión se realimenta de
> una copia vieja.

Documento de diseño del calendario distribuido y planificador de grupos.

> **Estado:** diseño congelado. A partir de aquí las dudas se resuelven en código
> y tests, no en otra ronda de revisión.
> **Consenso:** `hashicorp/raft` + `raft-boltdb/v2` (§17).
>
> **Registro de cambios:** 2026-11-03 — §6.1 fija el formato exacto de los
> bytes firmados (JCS del objeto de campos). Hash de la versión anterior:
> `405cf20a212e1ba19310049cb6927461d79de04c`.

---

## 1. Resumen

Sistema de calendario compartido con:

- Un **clúster de 3 nodos** dedicados que replican por Raft el estado de los
  eventos de grupo y las decisiones que requieren una única verdad.
- **Clientes local-first** que funcionan sin conexión y reconcilian al volver.
- **Autoridad por rol**: el organizador manda sobre los campos del evento, cada
  invitado manda solo sobre su respuesta.
- **Mensajes estilo iTIP** (`REQUEST`, `REPLY`, `COUNTER`, `CANCEL`) en lugar de
  estado compartido que se fusiona.
- **Sin CRDT en el MVP**: todo estado multi-escritor pasa por el log.

El objetivo académico es demostrar **consistencia, transacciones, identidad y
reconciliación** con un dominio que los ejercita de verdad.

## 2. Requisitos y trazabilidad

| Requisito del enunciado | Sección | Test |
| --- | --- | --- |
| Gestión de eventos personales | §4.4 | Propiedad: CRUD local idempotente |
| Invitaciones y reuniones de grupo | §5, §9 | Escenario: invitar → aceptar → confirmar |
| Flujos de aceptar y rechazar | §5.2, §5.3 | `REPLY` sobre cada estado (§5.4) |
| Consultas de disponibilidad | §10 | `BUSY_UPDATE` converge por `pub_seq` en los 3 nodos |
| Detección de conflictos de horario | §10.3 | Propiedad de solapamiento de intervalos |
| Controles de privacidad | §10.2 | `private`/`busy`/`details` por rol |
| Desconexión temporal | §11 | Outbox + `REFRESH` + replay idempotente |
| Sincronización y reconciliación | §11, §14 | Simulador con particiones |
| Propuesta / aceptada / confirmada | §5.3, §9 | `decisions` satisface su regla |
| *(transversal)* Consistencia | §13 | Linealizabilidad contra modelo secuencial |

## 3. Principios

| # | Principio |
| --- | --- |
| P1 | Un solo camino de escritura **para el estado autoritativo**: todo cambio es un comando firmado que entra al log. |
| P2 | La autoridad es un **rol verificado**, no una ubicación física. |
| P3 | Los campos del evento **nunca** son CRDT: escritor único + `SEQUENCE`. |
| P4 | La deduplicación vive en la máquina de estados replicada, no en memoria. |
| P5 | Ninguna lectura tiene efectos. Las decisiones se toman en el log. |
| P6 | El transporte es un puerto con dos adaptadores: real y simulado con fallos. |
| P7 | La máquina de estados es determinista: mismo log ⇒ mismo estado. |

## 4. Arquitectura general

```text
        ┌────────────┐
        │  Clientes  │  local-first, outbox, cursor de log
        └─────┬──────┘
              │ HTTPS/JSON
     ┌────────┼────────┐
     ▼        ▼        ▼
  ┌──────┐ ┌──────┐ ┌──────┐
  │ N1   │ │ N2   │ │ N3   │   Raft (RequestVote,
  │      │◄┤      ├►│      │   AppendEntries, Snapshot)
  └──────┘ └──────┘ └──────┘
         quórum = 2 · tolera 1 caída
```

- **3 nodos, membresía estática.** Con 2 caídos no hay quórum: las escrituras
  devuelven `503`, pero el nivel local-first sigue funcionando.
- **Los clientes no forman parte del quórum.**
- **El cliente no conoce al líder.** Habla con cualquier nodo; el follower
  reenvía internamente.
- **TLS obligatorio** cliente↔nodo y mTLS nodo↔nodo.

### 4.1 Dos planos

| Plano | Datos | Garantía |
| --- | --- | --- |
| **Autoritativo replicado** | Eventos de grupo, asistencias, propuestas, decisiones, registro de claves, ventanas `busy` | Linealizable |
| **Local (cliente)** | Calendario personal, outbox, overlays de propuestas | Sin coordinación |

No hay tercer plano. El antiguo "nivel 2 CRDT" se elimina: `COUNTER` vive en el
log y la disponibilidad es un último-valor-gana por `pub_seq` de dispositivo. Un
solo mecanismo de convergencia (el log) es menos superficie de bugs.

### 4.2 Por qué no CRDT

Un CRDT sobre los campos del evento permitiría que un invitado offline moviera
una reunión sin permiso, o que su cambio desapareciera en silencio. Ambos son
corrupción silenciosa: el desacuerdo nunca se hace visible. El log lo hace
visible y resoluble.

### 4.3 `uid`

```text
uid = organizer_id + ":" + ULID
```

El prefijo es **obligatorio**: un `REQUEST` con `sequence == 1` solo se acepta si
`uid` empieza por `sender_id + ":"`. Sin eso, quien vea un `uid` en una
invitación puede crearlo antes que su organizador.

**Creación vs. actualización:**

```text
sequence == 1  y uid desconocido → creación
sequence >  1  y uid desconocido → 409 (reordenación legítima del outbox)
```

Un `410` aquí perdería un cambio para siempre: la creación puede seguir esperando
en el outbox mientras una edición del mismo organizador llega antes. El cliente
hace `REFRESH`, re-emite y el outbox serializa **por `uid`** (un mensaje en vuelo
por evento).

**Tombstones permanentes.** `CANCELLED` y `EXPIRED` dejan una fila mínima
(`uid`, `state`, `cancelled_at`) que **no se recolecta en el MVP**. El diseño ya
conserva todos los eventos para siempre, que pesan cientos de bytes; 32 bytes por
tombstone no es un problema cualitativamente nuevo. Consecuencia asumida: el
snapshot es O(eventos totales), no O(eventos activos). El GC con horizonte queda
como trabajo futuro (§18).

Se descartó un `highwater` monótono por organizador: asume que el orden de los
ULID coincide con el de creación, y falla con outbox reordenado,
multidispositivo y relojes de cliente adelantados.

### 4.4 Calendario personal

Vive en el cliente, sin coordinación. Escritor único, sin CRDT. Publica ventanas
`busy` opacas al clúster (§10) y nunca los detalles.

## 5. Dominio y estados

### 5.1 Entidades

```text
User ── Device ── Key
Group ── GroupMember
Event (grupo) ── Attendance
              ── Counter
              ── Decision
Deployment ── Policy (singleton replicado)
```

### 5.2 Estados del evento

```text
PROPOSED ──(regla satisfecha)───────────> CONFIRMED
PROPOSED ──(EVALUATE falla)─────────────> NOT_CONFIRMED
PROPOSED ──(inicio sin decisión)────────> EXPIRED
NOT_CONFIRMED ──(inicio alcanzado)──────> EXPIRED
CONFIRMED | NOT_CONFIRMED ──(reopen)────> PROPOSED (sequence + 1)
* ──(CANCEL)────────────────────────────> CANCELLED
```

`EXPIRED` y `CANCELLED` son **terminales**. No se sale de ellos: para reintentar
la reunión se crea un evento nuevo con `uid` nuevo y `derived_from` apuntando al
anterior.

### 5.3 Estados de la participación

```text
NEEDS_ACTION ──ACCEPT────> ACCEPTED
NEEDS_ACTION ──DECLINE───> DECLINED
NEEDS_ACTION ──TENTATIVE─> TENTATIVE
```

- Mutables mientras el evento esté `PROPOSED` y no exista decisión.
- **`STALE_*`**: respuesta formada contra una versión material anterior. No
  aplica; el invitado queda en `NEEDS_ACTION` y debe responder de nuevo.
- **`LATE_*`**: respuesta posterior a la decisión. Informativa, sin efecto.

Son estados distintos con consecuencias distintas y nombres distintos.

`tentative` **nunca** cuenta como aceptado, en ninguna regla.

### 5.4 Matriz estado × comando

| Comando | PROPOSED | CONFIRMED | NOT_CONFIRMED | EXPIRED | CANCELLED |
| --- | --- | --- | --- | --- | --- |
| `REQUEST` material | aplica | **reopen implícito** | **reopen implícito** | `409` | `410` |
| `REQUEST` no material | aplica | aplica | aplica (sigue `NOT_CONFIRMED`) | `409` | `410` |
| `REPLY` | aplica | `LATE_*` | `LATE_*` | `410` | `410` |
| `COUNTER` | aplica | `409` | `409` | `410` | `410` |
| `CANCEL` | aplica | aplica | aplica | `409` | `200` (idempotente) |
| `REANCHOR` | aplica | aplica | `409` | `409` | `410` |
| `CLOSE_POLL` | aplica | `409` | `409` | `409` | `410` |
| `reopen` | `409` | aplica | aplica | `409` | `410` |
| `EVALUATE` (interno) | aplica | no-op | no-op | no-op | no-op |
| `BUSY_UPDATE` | n/a | n/a | n/a | n/a | n/a |

`BUSY_UPDATE` es por usuario y ventana, no por evento: no consulta el estado del
evento y se aplica siempre (sujeto a `pub_seq`).

### 5.5 `reopen`

Firmado por el organizador. `SEQUENCE+1`, estado → `PROPOSED`, `deadline`
recalculado con el clamp de §9.6. Máximo **3 reaperturas** por evento (`409` al
superarlo).

| Modo | `PARTSTAT` | `LATE_*` previos | Roster |
| --- | --- | --- | --- |
| `reask` (default) | Todos a `NEEDS_ACTION` | Se descartan | Se descongela: ronda nueva |
| `extend` | Se conservan | **Se promueven a `PARTSTAT`** | Sigue congelado |

`extend` promueve los `LATE_*` porque ese es su caso de uso: alguien respondió
después del deadline y el organizador amplía el plazo para que su respuesta
cuente. Si no se promovieran, `extend` no serviría para nada.

En `reask`, el organizador **puede** purgar del roster a quienes votaron en la
ronda anterior: todos están en `NEEDS_ACTION`, así que son removibles. Se permite
porque prohibirlo no protege —el organizador puede crear un evento nuevo con otro
roster— y el tope de 3 más el log sí acotan el abuso.

### 5.6 Cambio material (`materiality v1`)

**La materialidad se juzga sobre la intención canónica**, nunca sobre los
instantes UTC resueltos:

| Cambio | Tipo |
| --- | --- |
| `start_local`, `end_local`, `all_day`, `tzid`, `location` | **Material** |
| `start_utc`, `end_utc` (re-resolución con la misma `start_local` + `tzid`) | **No material** |
| `summary`, `description`, `url`, `attachments` | No material |

Esto permite **re-anclar tras una actualización de tzdata** sin resetear los
`PARTSTAT`. La tabla de materialidad está versionada (`materiality_version`) y se
guarda en el evento, no en la regla de confirmación.

Cada cambio material fija `anchored_start_utc` con el nuevo `start_utc`. Es la
base de la cota de `REANCHOR`.

Un cambio material en `CONFIRMED` produce reapertura implícita (§5.4), no un
error.

#### 5.6.1 `REANCHOR`

Si los UTC son opacos para la FSM, nadie valida que local y UTC sean coherentes.
`REANCHOR` es el comando que lo acota:

```text
REANCHOR(uid, sequence, start_utc, end_utc, tz_version)
  firma:    organizador
  estados:  PROPOSED | CONFIRMED
  cota:     |start_utc − anchored_start_utc| <= 3 h   (DST real: <= 2 h)
  tz_version: estrictamente creciente
  valida:   deadline_at < nuevo start_utc
  efecto:   actualiza instantes y tz_version; NO toca deadline_at,
            NO toca PARTSTAT, NO toca el roster
  avisa:    notificación a los invitados
```

Tres reglas que cierran los huecos:

1. **La cota es acumulada, no por comando.** Se mide contra
   `anchored_start_utc`, no contra la versión anterior. Si se midiera por
   comando, encadenar `REANCHOR`s de 2 h 59 min movería el evento a cualquier
   hora sin resetear `PARTSTAT`.
2. **Consume versión.** Toda mutación sube `SEQUENCE`; como es **no material**,
   `last_material_sequence` no se mueve y los `REPLY` en vuelo siguen válidos.
3. **No recalcula el deadline.** Si recalculara, podría moverlo y descongelar el
   roster. Si el invariante `deadline_at < start_utc` se rompe, el comando se
   rechaza con `409`: en `PROPOSED` se corrige con un `REQUEST` material; en
   `CONFIRMED`, con `reopen`.

### 5.7 Tiempo: intención y operación

El evento almacena **ambas** representaciones:

```text
start_local, end_local, tzid   -- intención canónica (para mostrar y re-resolver)
start_utc, end_utc             -- instantes opacos para la FSM (deadline, expiración)
tz_version                     -- versión de tzdata con la que se resolvió (informativa)
```

**La FSM nunca convierte zonas horarias.** Recibe los instantes ya resueltos por
el cliente y los compara como opacos. Si un país cambia su regla de DST, el
organizador re-ancla con `REANCHOR` (§5.6.1) y los clientes recalculan.

`all_day`: `start_date`/`end_date` locales + `TZID`. El cliente aporta
`deadline_utc` como **dato opaco**:

```text
deadline_utc = clamp(
    max(09:00 local del día anterior, invited_at + grace),
    invited_at + 1 min,
    start_utc − 1 min )
```

La FSM no convierte zonas horarias: solo valida
`invited_at < deadline_utc < start_utc`. Si el cliente manda un `deadline_utc`
que rompe el invariante, el `REQUEST` se rechaza con `422`.

## 6. Protocolo

### 6.1 Sobre de mensaje

```text
msg_id           ULID (único; solo unicidad, nunca orden causal)
deployment_id    identificador del despliegue
group_id         grupo del evento
type             REQUEST | REPLY | COUNTER | DECLINECOUNTER | CANCEL
                 | CLOSE_POLL | REOPEN | BUSY_UPDATE
uid              UID canónico
sequence         versión del evento (REQUEST/CANCEL)
seen_sequence    versión vista por el emisor (REPLY/COUNTER)
sender_id        usuario emisor
key_id           clave usada para firmar
client_sent_at   timestamp del cliente — informativo, nunca ordena
signature        Ed25519
payload          campos del tipo
```

**Bytes firmados.** No es una concatenación con separadores: es el **JCS
(RFC 8785) del objeto** cuyos campos son exactamente los de la lista anterior,
sin `signature`.

```json
{"client_sent_at":"…","deployment_id":"…","group_id":"…","key_id":"…",
 "msg_id":"…","payload":{…},"seen_sequence":0,"sender_id":"…",
 "sequence":1,"type":"REQUEST","uid":"…"}
```

`payload` se canonicaliza como **objeto** (no como string) antes de insertarse.
JCS ordena las claves, así que el orden textual de arriba no importa; lo que
queda fijado son los nombres, que todos los campos están siempre presentes
(incluidos los cero) y que los números son enteros. Los vectores dorados viven
en `pkg/protocol/envelope_test.go`
(`TestSigningInputGoldenVector` y `TestSignatureGoldenVector`): fijan los bytes
exactos y la firma de una semilla conocida, y fallan si el formato cambia.

`deployment_id` y `group_id` cierran el replay cruzado entre despliegues y entre
grupos. `key_id` va **dentro** de la firma para impedir sustitución de clave.
`proposed_at` y `log_index` los asigna el líder y quedan **fuera**.

### 6.2 Tipos

| Tipo | Emisor | Efecto |
| --- | --- | --- |
| `REQUEST` | Organizador | Crea/actualiza el evento |
| `REPLY` | Invitado | Fija su `PARTSTAT` |
| `COUNTER` | Invitado | Propone un cambio; no toca el evento |
| `DECLINECOUNTER` | Organizador | Rechaza la propuesta |
| `CANCEL` | Organizador | Tombstone |
| `CLOSE_POLL` | Organizador | Cierra la votación y decide según la regla |
| `REOPEN` | Organizador | Devuelve a `PROPOSED` |
| `REANCHOR` | Organizador | Re-resuelve instantes tras cambio de tzdata |
| `BUSY_UPDATE` | Cualquiera | Publica ventanas `busy` de su calendario |
| `EVALUATE` | **Interno del líder** | Vencimiento de deadline o inicio |

`EVALUATE` **no tiene endpoint de cliente y no se firma**: su validez es función
de `(estado, proposed_at)`. Cualquier nodo puede proponerlo al ser líder; la FSM
rechaza los prematuros.

Consecuencia honesta: al no ir firmado, `EVALUATE` está protegido **solo por la
honestidad del líder**, igual que `proposed_at`. Es coherente con el modelo de
amenaza (§8.3), pero la re-verificación de la FSM que describe §7.1 aplica a
**comandos firmados**; para los de sistema solo puede validar el estado.

### 6.3 Política de obsolescencia

```text
REQUEST   : sequence == 1 y uid desconocido  → creación (exige prefijo organizer:)
            sequence >  1 y uid desconocido  → 409 (outbox reordenado)
            sequence != current + 1          → 409
CANCEL / REOPEN / REANCHOR : sequence == current + 1, si no 409
REPLY     : nunca 409 por sequence
            seen_sequence < last_material_sequence → STALE_*
            seen_sequence > current                → 422
COUNTER   : base_sequence < current → superseded (sin efecto)
```

`seen_sequence > current` no puede ocurrir con lecturas linealizables: si se
observa, es un bug de linealizabilidad y la suite debe detectarlo, no un caso de
uso.

### 6.4 Códigos de respuesta

| Código | Cuándo | Acción del cliente |
| --- | --- | --- |
| `200` | Commit, o duplicado byte-idéntico con respuesta cacheada | Ack y borrar del outbox |
| `401` | Firma inválida, clave desconocida o expirada | No reintentar; reautenticar |
| `403` | Identidad válida, rol insuficiente | No reintentar |
| `409` | `sequence` incorrecta, estado no permite, roster congelado, `uid` desconocido con `sequence > 1` | `REFRESH` y re-emitir |
| `410` | Evento cancelado o expirado | Descartar y registrar |
| `422` | Payload inválido, reutilización de `msg_id` con otro contenido | Bug del cliente |
| `429` | Rate limit (admisión, por nodo, no determina estado) | Backoff |
| `503` | Sin líder, sin quórum, lectura no confirmable | **Reintentar el mismo `msg_id`** |

## 7. Máquina de estados replicada

### 7.1 Algoritmo de admisión y `apply`

El orden importa: la **firma criptográfica** se verifica antes que la validez
temporal, para que un reintento tras una rotación de clave siga recibiendo su
respuesta cacheada.

```text
ADMISIÓN (líder, sin escribir en el log)
1. parseo y firma criptográfica con la pubkey de key_id     → 401 si no verifica
2. dedupe: msg_id + hash de los bytes firmados SIN la firma
       coincidencia → devolver respuesta almacenada (200/409/...)
       msg_id igual, hash distinto → 422
3. validez temporal de la clave en proposed_at              → 401 si expirada
4. authz: rol y pertenencia                                 → 403
5. política de sequence y estado (§5.4, §6.3)               → 409/410
6. proponer al log

APPLY (los 3 nodos, determinista)
1. NOOP → return
2. dedupe replicada (misma regla que en admisión)
3. re-verificación de firma y rol con el key_id de la entrada
4. mutación de estado
5. guardar (msg_id, bytes_hash, response)
6. si el mensaje habilita una decisión, escribirla
```

Un duplicado **byte-idéntico** no puede fabricarlo quien no posee la firma, así
que devolver la respuesta cacheada no filtra la existencia de `msg_id`.

Se compara el hash de los bytes firmados **sin la firma**: Ed25519 es determinista
(RFC 8032), pero no todas las librerías producen la misma firma válida para los
mismos bytes. La firma se verifica antes de mirar el hash, así que excluirla no
abre nada.

El outbox guarda el mensaje **ya firmado**. Re-firmar tras una rotación exige
`msg_id` nuevo; reutilizar un `msg_id` con otra firma es `422`.

### 7.2 Determinismo

| Regla | Motivo |
| --- | --- |
| Prohibido `now()` en `apply` | Solo `proposed_at`, asignado por el líder |
| Prohibido `random()` y generación de IDs en `apply` | Cada nodo generaría otro |
| Estructuras ordenadas en el snapshot | Iteración de mapas no es determinista |
| Serialización canónica | Firmas y hashes estables |
| Sin conversión de zonas horarias en la FSM | tzdata fuera del camino crítico |
| Comandos de sistema sin re-verificación criptográfica | `EVALUATE` se valida por `(estado, proposed_at)` |
| Sin coma flotante en las reglas | Enteros, sin `round()` |
| Snapshot ≡ replay | Test de propiedad obligatorio |

### 7.3 `proposed_at` y cota de reloj

```text
líder:  proposed_at = max(leader_now, prev_proposed_at)
apply:  si proposed_at < último aplicado → usar el último (clamp determinista)

muestras = { 0 } ∪ { offset_f1, offset_f2 }     # offset medido en el heartbeat, corregido por RTT
referencia = mediana(muestras)
el líder abdica si |0 − referencia| > MAX_SKEW (5 s)
```

**La mediana incluye el propio líder.** Con las muestras de los followers solas,
un solo desviado desplaza el punto medio Δ/2 y provoca abdicaciones falsas a
mitad de umbral. Con tres muestras:

| Escenario | Muestras | Mediana | ¿Abdica? |
| --- | --- | --- | --- |
| Todo correcto | `{0, +0.1, −0.1}` | 0 | No |
| Un follower malo | `{0, +10, +0.1}` | +0.1 | **No** |
| El líder malo | `{0, −10, −10}` | −10 | **Sí** |

- **El líder es el único que acota.** Los followers aplican siempre: rechazar
  una entrada por el reloj local divergiría el estado y rompería la seguridad de
  Raft.
- Si solo hay **un follower** alcanzable, dos muestras no permiten saber quién
  miente. En ese caso **no se abdica** y no se propone `EVALUATE`; se registra la
  anomalía. Líder + un follower que coinciden son 2 de 3 y **bastan**.
- No hace falta unicidad ni `+1 ms`: `max` basta y no adelanta el tiempo lógico
  en ráfagas.
- `CheckQuorum` **no** protege del desvío de reloj: detecta pérdida de contacto,
  no mala hora.
- Supuesto explícito: relojes acotados por NTP. Un líder adelantado decide antes;
  nunca produce desacuerdo entre nodos, solo decisiones prematuras que `reopen`
  corrige.

## 8. Identidad y claves

### 8.1 Raíz de confianza

Cada nodo lleva en su configuración la clave raíz del despliegue:

```toml
root_key_id = "root-1"
root_pubkey = "ed25519:..."
```

La entrada **0 del log es el génesis**, firmado por root. Un nodo que no puede
validarlo **no sirve**.

| Operación | Firmada por |
| --- | --- |
| `RegisterDevice` (primera vez) | root |
| `RotateKey` | la clave vigente del propio dispositivo |
| `RevokeKey` | root (o clave de recuperación) |

La recuperación de clave perdida la resuelve root. Sin flujo de preguntas de
seguridad.

### 8.2 Claves por dispositivo

```sql
devices(device_id, user_id, key_id, pubkey,
        valid_from, valid_to, revoked_at)
```

- `RotateKey` afecta **solo a ese dispositivo**; los demás siguen con su clave.
- `RevokeKey` es **inmediata**: entradas con `proposed_at > revoked_at` → `401`.
- `RotateKey` **no invalida** la clave anterior de golpe: sigue siendo válida
  para verificar hasta `rotate.proposed_at + rotation_grace` (config replicada,
  por defecto 7 días). Cierra la ventana del outbox offline y la circularidad
  firma/índice.
- Las claves **revocadas se conservan** con su `pubkey` en el registro: sin eso
  no se puede verificar la firma de un reintento legítimo.

### 8.3 Riesgos asumidos

- Un dispositivo comprometido puede **rotar** la clave y bloquear al dueño hasta
  que root recupere. Se acepta y se documenta.
- Las entradas firmadas antes de la revocación **siguen siendo válidas**: no se
  reescribe la historia. El cliente marca el evento como "firmado por clave
  revocada" si quiere ser transparente.
- El operador del despliegue controla la clave raíz. **Raft no da tolerancia
  bizantina entre nodos.**

## 9. Reglas de confirmación

### 9.1 Regla versionada

```sql
confirmation_rules(
  rule_id PK, version, kind, n,
  organizer_counts, min_grace_min
)  -- inmutable: cambiarla crea fila nueva

events.rule_id → confirmation_rules.rule_id
events.materiality_version
CHECK (kind = 'QUORUM') = (n IS NOT NULL)
```

Sin `include_optional`: los `OPTIONAL` nunca cuentan. Una opción menos.

Cada evento referencia una regla inmutable. Cambiar la regla del grupo no
re-evalúa eventos ya creados.

### 9.2 Conjunto elegible

```text
eligible = { a : a.role = REQUIRED } ∪ ({organizer} si organizer_counts)
```

- `organizer_counts` es `true` por defecto: el organizador cuenta como aceptado
  implícito, sin `PARTSTAT`, sin fila de asistencia.
- `INVITE` exige **≥ 1 `REQUIRED` distinto del organizador** → `422` si no. Una
  reunión sin invitados no es una reunión.
- `NEEDS_ACTION` cuenta en el denominador: si no, un solo "sí" de diez invitados
  podría decidir.

### 9.3 Fórmulas

```text
|eligible|  tamaño del conjunto elegible

ALL         : confirmed ⟺ accepted == |eligible|
QUORUM(n)   : confirmed ⟺ accepted >= n
MAJORITY    : confirmed ⟺ 2·accepted > |eligible|
ORGANIZER   : nunca automática
```

Todo entero. `MAJORITY` estricta elimina el empate por construcción. `tentative`
nunca suma en `accepted`.

### 9.4 Disparadores

```text
1. REPLY que satisface la regla        → CONFIRMED (derivada al aplicar)
2. EVALUATE con deadline vencido       → CONFIRMED | NOT_CONFIRMED
3. EVALUATE con inicio alcanzado       → EXPIRED
4. CLOSE_POLL del organizador          → decide según la regla
5. CANCEL / REOPEN                     → transición directa
```

- **No hay fallo temprano.** `NOT_CONFIRMED` es destructivo; `CONFIRMED` es
  constructivo y su fallo ya lo cubren las respuestas tardías.
- **No hay override.** Un `CLOSE_POLL` que no satisface la regla produce
  `NOT_CONFIRMED`. Para forzar, el grupo usa la regla `ORGANIZER`.
- `EVALUATE` no lleva `reason`: la FSM lo deriva del estado y de `proposed_at`.
- La ventana deadline→`EVALUATE` cuenta **por orden de log**. Un cambio de líder
  no alarga el plazo.
- Sin quórum al vencer el deadline no hay decisión: el evento queda visible como
  **pendiente**, no como `NOT_CONFIRMED`. Es el tercer estado de la UI.
- **Puerta de reloj para `EVALUATE`.** Una transición destructiva no puede
  depender de un reloj: el líder solo propone `EVALUATE(deadline)` si existe una
  mayoría de relojes (líder incluido) que coincidan dentro de `MAX_SKEW` y cuya
  mediana ya pasó `deadline_at`. Si los relojes discrepan, espera y alerta.

### 9.5 `EVALUATE` determinista

```json
{ "type": "EVALUATE", "uid": "evt-123", "sequence": 4 }
```

La FSM recalcula counts y decisión desde su propio estado. Un `expected_counts`
opcional es **no autoritativo**: si no coincide, se registra una anomalía y se
aplica el recálculo.

Duplicado para `(uid, sequence)` → no-op.

### 9.6 Deadline

```text
span = start_utc − invited_at
si span <= 2 min                     → 422
grace = min(min_grace, span / 2)
deadline = min(start_utc − 1 min,
               max(start_utc − rsvp_window, invited_at + grace))

invariante: invited_at < deadline < start_utc
```

Un evento que empieza en el pasado → `422`, no clamp.

Para `all_day`, el cliente calcula `deadline_utc` según §5.7 y la FSM solo valida
el invariante. No hay conversión de zona horaria en la máquina de estados.

### 9.7 Roster

| Comando | Permitido | Restricción |
| --- | --- | --- |
| `INVITE` | Al crear | ≥ 1 `REQUIRED` no-organizador; valida `QUORUM(n ≤ eligible)` |
| `ADD_ATTENDEE` REQUIRED | `PROPOSED` y `proposed_at < deadline` | Congelado al vencer el primer deadline; `extend` no lo descongela |
| `ADD_ATTENDEE` OPTIONAL | `PROPOSED` siempre | Nunca cuenta |
| `REMOVE_ATTENDEE` | `PROPOSED`, solo si no ha respondido | `409` si respondió; `409` si rompería `QUORUM(n)` |

El roster `REQUIRED` se congela en el **primer** `deadline` y `extend` no lo
descongela. Cierra el exploit de añadir un amigo en el último segundo para
voltear `MAJORITY`, también a través de una reapertura. Solo `reask` abre ronda
nueva, y ahí nadie arrastra su voto.

El organizador **nunca** es una fila de asistencia: si aparece en `REQUEST`, se
ignora silenciosamente (no es un error del cliente).

### 9.8 Decisión auditable

```sql
decisions(uid, sequence, trigger, rule_id, roster_hash,
          accepted, declined, needs_action, tentative,
          decision, expected_counts_match, log_index, proposed_at,
          PRIMARY KEY (uid, sequence))
```

Verificación = replay hasta `log_index` + recálculo con el mismo código que usa
un nodo al arrancar. El verificador no puede estar desviado del sistema.

## 10. Disponibilidad y privacidad

### 10.1 Publicación de `busy`

Las ventanas viajan por el log, pero **no con LWW por orden de log**: un
`BUSY_UPDATE` viejo reintentado tras un `503` pisaría a uno nuevo, que es el
mismo reordenamiento que descartamos para `uid`.

```sql
busy_windows(user_id, device_id, window_start,
             window_end, intervals, pub_seq, published_at,
             PRIMARY KEY (user_id, device_id, window_start))

last_pub_seq(device_id, seq)   -- replicado
```

- **Orden por `pub_seq` monótono por dispositivo**, dentro del payload firmado.
  La FSM aplica solo si `pub_seq > last_pub_seq(device)`. Un dispositivo tiene un
  único escritor, así que el contador es seguro; una reinstalación implica
  `device_id` nuevo.
- Clave `(user_id, device_id, window_start)`; al leer se **unen** las ventanas de
  todos los dispositivos del usuario. Horizonte 60 días, ventanas semanales.
- El snapshot conserva solo la última versión de cada ventana: el coste no crece
  con el tiempo.
- Se publica por lotes, y solo cuando cambian los intervalos. La edición personal
  sigue siendo local e instantánea: lo que se replica es **su proyección**, de
  forma asíncrona.
- Un dispositivo que deja de publicar deja `busy` fantasma. Se filtra en lectura
  por `published_at` (`device_stale_after`, por defecto 30 días) y `RevokeKey`
  limpia sus ventanas.
- Reintroducir el "nivel 2 CRDT" para esto sería irónico: el log ya converge.

**`stale` no basta como señal.** Mezcla "el usuario no publicó" con "este nodo va
atrasado". La respuesta de `/v1/availability` lleva `as_of` (último
`proposed_at` aplicado) además del flag, y el cliente distingue los dos casos.

`stale` es **advisory y local al nodo**: dos nodos pueden discrepar durante
milisegundos y no pasa nada, porque no es estado. `as_of` es lo que se compara.

### 10.2 Privacidad

| Nivel | Qué ve un tercero |
| --- | --- |
| `private` | `404` |
| `busy` | Solo intervalos, sin `uid`, sin título |
| `details` | Todo |

`private` significa "no puedes leer el evento", no "no existe la ocupación": el
`404` aplica a `/v1/events/{uid}`, mientras que su ventana sigue siendo visible
en `/v1/availability` bajo la regla siguiente.

- `/v1/availability` devuelve **solo** intervalos `busy`.
- **Todo evento personal contribuye una ventana `busy` opaca, sea cual sea su
  nivel de privacidad.** La privacidad gobierna el detalle del evento, no la
  ocupación: si un `private` no bloqueara, la detección de conflictos sería
  ciega y los demás agendarían encima. Existe un flag explícito
  `hide_from_freebusy` para quien quiera confidencialidad total y acepte ese
  riesgo.
- El filtrado **no renumera**: el `log_index` se conserva aunque se omitan
  entradas.
- Canal lateral conocido: publicar `busy` revela patrones de vida (horarios,
  ausencias). Es una decisión de producto consciente; se documenta.

### 10.3 Conflictos de horario

Derivados **en el cliente**: solapamiento de intervalos entre el calendario
personal y los eventos aceptados. No hay mecanismo en el servidor ni estado
nuevo. Al proponer una reunión, el cliente del organizador la comprueba contra
su propia vista antes de enviar el `REQUEST`.

## 11. Sincronización offline

```text
cliente:  outbox(msg_id, payload, attempts, next_retry_at)
          inbox_seen(msg_id)
          sync_cursor(group_id, last_log_index)

push:      POST /v1/commands     (idempotente por msg_id)
pull:      GET  /v1/sync?since=N (monotónica + read-your-writes)
recuperar: REFRESH tras desconexión larga
```

- `503` → reintentar con el **mismo `msg_id`**.
- `409` → `REFRESH`, reconciliar, re-emitir con `msg_id` nuevo. Si el `409` es
  `uid` desconocido con `sequence > 1`, el outbox re-emite primero la creación.
- `401` por rotación → el mensaje se re-firma con la clave nueva y **nuevo**
  `msg_id`; el anterior se descarta (no commiteó).
- **El outbox serializa por `uid`**: un solo mensaje en vuelo por evento. Es lo
  que hace imposible el reordenamiento entre creación y actualización.
- Ventana de rotación de 7 días por defecto: cubre el outbox de un cliente
  desconectado sin obligar a coordinar.
- **`BUSY_UPDATE` queda fuera de `/v1/sync`.** Si viajara por el cursor, cada
  cliente recibiría las ventanas de ocupación de todos los demás. Se lee por
  `/v1/availability`, que aplica privacidad.

## 12. API

| Método | Ruta | Consistencia |
| --- | --- | --- |
| `POST` | `/v1/commands` | Linealizable |
| `GET` | `/v1/sync?since=N&limit=M` | Monotónica + RYW |
| `GET` | `/v1/events/{uid}?at_least=N` | Linealizable (ReadIndex) |
| `GET` | `/v1/availability?users=…&from=…&to=…` | Monotónica |
| `GET` | `/v1/status` | Local |

### 12.1 Sobre de respuesta

Toda respuesta lleva la posición en el log, porque el checker de
linealizabilidad no puede modelar el tiempo sin ella:

```json
{ "status": "committed", "log_index": 1042, "proposed_at": "2026-11-02T09:12:03Z" }
```

- `log_index` permite situar la operación en el orden total.
- `proposed_at` permite reproducir deadline, congelado y expiración en el modelo
  secuencial.
- Las lecturas devuelven `read_index` (el índice desde el que se sirvió) para
  poder situarlas también.

El checker contrasta el `log_index` declarado con el log commiteado de los nodos:
es un chequeo con testigo, no una garantía por sí mismo.

### 12.2 Autenticación

- Escrituras: firma Ed25519 del comando (no hay token de sesión para escribir).
- Lecturas: `POST /v1/session` con un nonce firmado por el dispositivo → token
  con TTL, verificado por **HMAC con secreto de clúster en config**. Los tokens
  no van al log; son autenticación de nodo, no estado replicado.
- `at_least=N` mayor que `commit_index` → `422`; timeout → `503`.

## 13. Consenso

- Raft estándar: `RequestVote`, `AppendEntries`, `InstallSnapshot`.
- **Módulo separado** `consensus/` con su propia suite de conformidad.
- **Lecturas lineales: `ReadIndex`.** `CheckQuorum` se usa como liveness (evitar
  líderes zombis), no como sustituto. Un líder que no puede confirmar con
  quórum **deja de servir lecturas lineales en el acto** y devuelve `503`.
- Snapshots + compactación obligatorios. El snapshot incluye **todo** el estado:
  `applied_msgs`, registro de claves con revocadas, tombstone de `uid`,
  `busy_windows` y `last_pub_seq`.
- Con librería, `fsync` y persistencia durable los cubre el almacén
  (`raft-boltdb`). **El snapshot sigue siendo nuestro** y debe ser consistente
  con `apply`: copy-on-write o doble buffer. Un snapshot tomado en mitad de una
  aplicación produce un estado inexistente.

### Matriz de fallos

| Fallo | Efecto | Esperado |
| --- | --- | --- |
| 1 nodo caído | Quórum 2/3 | Sin impacto perceptible |
| 2 nodos caídos | Sin quórum | Escrituras `503`; evento "pendiente", no fallido |
| Líder aislado | Minoría | No commitea, no sirve lecturas lineales |
| Cliente offline | — | Outbox + `REFRESH` + replay idempotente |
| Reloj del líder desviado | Tiempo lógico desplazado | Alerta; decisiones prematuras corregibles por `reopen` |
| Mensaje duplicado | — | Dedupe replicada lo absorbe |

## 14. Testing

| Test | Propiedad |
| --- | --- |
| Replay ≡ snapshot | Para todo log aleatorio, `replay(log) == estado` |
| Decisión válida | Toda fila de `decisions` satisface su `rule_id` |
| Idempotencia | Duplicado byte-idéntico devuelve la misma respuesta |
| Duplicado tras rotación | Un reintento con clave rotada recibe su `200` cacheado |
| Congelado del roster | `ADD_REQUIRED` tras deadline → `409` |
| Inmutabilidad post-decisión | Todo `REPLY` posterior → `LATE_*` |
| Invariante de deadline | `invited_at < deadline < start_utc` |
| Serialización canónica | Round-trip estable; firmas verifican igual |
| **Linealizabilidad** | Historia completa contra el **modelo secuencial de la FSM**, no por `uid`: hay operaciones multi-objeto (registro de claves) |
| **Líder viejo tras partición** | Entradas no commiteadas se descartan sin efecto en clientes |
| **Compatibilidad de versiones** | Un log v1 se reproduce con la FSM actual |
| `BUSY_UPDATE` por `pub_seq` | Un update viejo reintentado no pisa al nuevo |
| Ventanas de usuario | La unión por dispositivo converge en los 3 nodos |
| `uid` reordenado | Creación y actualización fuera de orden no pierden cambios |
| `REANCHOR` encadenado | La cota acumulada impide mover el evento sin materialidad |
| `all_day` | El `deadline_utc` del cliente respeta `invited_at < deadline < start_utc` |
| Mediana de reloj | Un follower desviado no provoca abdicación |
| Conflicto de horario | Propiedad de solapamiento |
| `fsync` (si Raft propio) | Matar el proceso y reiniciar no pierde entradas commiteadas |

El `SimulatedTransport` es un **simulador de eventos discretos
single-threaded** con PRNG sembrado: no se puede sembrar el planificador de Go o
de Tokio. Inyecta partición, duplicado, reordenación, latencia, caída de nodo y
desvío de reloj. **Se implementa antes que la FSM**: sin él, los tests de
propiedad no son reproducibles.

La linealizabilidad se comprueba contra el modelo secuencial completo porque la
propiedad es local y composicional, pero las **operaciones no lo son**: todo
comando lee el registro de claves además del evento. Particionar por `uid` solo
es válido tratando los comandos de claves como barreras.

## 15. Alcance por fases

| Fase | Contenido |
| --- | --- |
| **Núcleo** | Log replicado, FSM determinista, dedupe, `REQUEST`/`REPLY`/`CANCEL`, `ALL`/`MAJORITY`, deadline + `EVALUATE` + `EXPIRED`, roster congelado, simulador sembrado, test replay ≡ snapshot |
| **Segunda ola** | `COUNTER`, snapshots y compactación, `BUSY_UPDATE` + privacidad, `CLOSE_POLL`, `REOPEN`, `QUORUM`, checker de linealizabilidad |
| **Recortable** | Rotación de claves (basta registro estático + revocación), multi-dispositivo, interoperabilidad iCalendar completa, recurrencia |

**No recortable** (está en el enunciado): eventos personales, invitaciones,
aceptar/rechazar, disponibilidad, conflictos, privacidad, offline, sync,
distinción propuesta/aceptada/confirmada.

El roster congelado y el deadline van al núcleo aunque sean "detalle": son ~10
líneas y sin ellos `MAJORITY` no se puede probar.

## 16. Decisiones cerradas

| Decisión | Elección | Motivo |
| --- | --- | --- |
| Arquitectura | Cliente + 3 nodos Raft | Tolerancia a fallo del organizador |
| Raft sobre | Servidores dedicados | Dispositivos no dan quórum estable |
| CRDT | **Ninguno en el MVP** | Un solo mecanismo de convergencia |
| Campos del evento | Escritor único + `SEQUENCE` | CRDT permitiría merges que violan la autoridad |
| Tiempo canónico | Local + `TZID` (intención) y UTC (opaco) | tzdata fuera de la FSM |
| Materialidad | Sobre la intención local | Re-resolución por tzdata no resetea RSVP |
| Firma | Admisión + `apply` | Raft no protege de un líder comprometido |
| Orden de checks | Firma cripto → dedupe → validez temporal | Mantiene la idempotencia del outbox |
| Rotación de clave | Δ de 7 días; revocación inmediata | Cubre clientes offline sin abrir compromiso |
| Claves revocadas | Se retienen para verificar | Si no, el reintento legítimo no verifica |
| `tentative` | No cuenta como aceptado | "Quizá" no es "sí" |
| Fallo temprano | No existe | `NOT_CONFIRMED` es destructivo |
| Override | No existe | Rompe la auditabilidad |
| Roster | Congelado en el primer `deadline` | Evita manipulación de última hora, incluso vía `extend` |
| Terminales | `EXPIRED`/`CANCELLED` no salen | Reintento con `uid` nuevo |
| `uid` | Prefijo obligatorio del organizador; sin `highwater` | El orden de ULID no coincide con el de creación |
| Tombstones | Permanentes en el MVP | 32 bytes frente a eventos de cientos |
| Reloj | Mediana de `{0, f1, f2}`; abdica solo el desviado | Robustez a un nodo malo |
| `EVALUATE` | Interno, sin firma ni `reason`; puerta de quórum de relojes | Validez por estado, no por emisor |
| `REANCHOR` | Cota acumulada, `tz_version` creciente, no toca deadline | Evita mover el evento en cadenas |
| `reopen` | `extend` promueve `LATE_*`; `reask` abre ronda; tope 3 | Cierra el exploit del roster |
| Dedupe | Hash sin la firma | Independiente de la librería de Ed25519 |
| `sync` | Monotónica + RYW; `BUSY_UPDATE` excluido | ReadIndex por poll no escala; privacidad |
| Disponibilidad | LWW por `pub_seq` de dispositivo | El orden de log no refleja la intención |
| Linealizabilidad | Modelo secuencial completo con `log_index` y `proposed_at` | Operaciones multi-objeto y tiempo |

## 17. Pendiente

### Consenso: **librería** (decidido)

La rúbrica no exige implementar el algoritmo, así que se usa una librería y el
esfuerzo va al sistema: FSM, protocolo, reconciliación y tests.

| Consecuencia | Efecto |
| --- | --- |
| `consensus/` se mantiene como límite | La FSM no conoce el transporte ni el log |
| `fsync` y almacenamiento durable | Los resuelve la librería (`raft-boltdb`) |
| Snapshot consistente con `apply` | **Sigue siendo responsabilidad nuestra** |
| Tests de consenso | Conformidad de la librería + matriz de fallos propia |
| Linearizabilidad | Se verifica con un checker externo, no se implementa |

**Librería concreta: `hashicorp/raft`** con `raft-boltdb/v2`. Es el Raft de
Consul y Nomad, y arrastra `anishathalye/porcupine` para el checker de
linealizabilidad.

### Stack: **Go + Wails** (decidido)

```text
server/   Go + hashicorp/raft + boltdb   → 3 nodos en docker-compose
client/   Go (store, outbox, firma) + Wails (ventana webview)
pkg/      protocol/ compartido por ambos binarios
```

El criterio que decide no es el rendimiento: es que **el sobre de mensaje, la
serialización canónica y Ed25519 se implementan una sola vez**. Con dos lenguajes
habría dos implementaciones que deben producir bytes idénticos; cualquier
divergencia rompe la firma y aparece como un bug de consenso.

| Alternativa descartada | Motivo |
| --- | --- |
| Rust + `openraft` | API mucho más dura, sin ganancia en el cliente |
| Flutter desktop | Segunda implementación de crypto/canonical en Dart |
| Python + `pysyncobj` | No hay Raft de producción en Python |
| Go + navegador local | Funciona, pero es una pestaña, no una app |

**Consecuencias asumidas:**

- Wails en Linux exige `libwebkit2gtk-4.1-dev` para compilar, y usa WebKitGTK.
- El calendario personal vive **solo** en el SQLite local del cliente
  (`~/.local/share/sdcal/sdcal.db`). Pérdida de disco = pérdida de datos. Se
  mitiga con export/import `.ics` (§18).
- `BUSY_UPDATE` y el log quedan en infraestructura de confianza: los tres nodos
  ven las ventanas de ocupación.

### Ventanas de la UI (Wails v3)

Decisión de producto: **una sola ventana principal** con navegación interna y
modales en la propia página. Las ventanas secundarias de Wails v3 existen y son
nativas, pero `AttachModal` solo funciona en macOS, así que construir la UX
sobre ventanas hijas daría comportamiento distinto en Linux.

| Necesidad | Implementación |
| --- | --- |
| Vista mes / semana / día | Rutas internas en la ventana principal |
| Editar evento, responder invitación | Modal en página (no ventana) |
| Ajustes | Ventana secundaria opcional, singleton por nombre |
| Diálogos de fichero (`.ics`) | Diálogos nativos de Wails |

## 18. Trabajo futuro

- Recurrencia (`RRULE`) y `RECURRENCE-ID`.
- Interoperabilidad iCalendar completa (`VEVENT`, `ATTENDEE`, `VFREEBUSY`).
- CRDT para notas y comentarios multi-escritor, si el producto lo pide.
- Reconfiguración dinámica del clúster.
- Sincronización P2P entre clientes.
