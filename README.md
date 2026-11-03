# SD Calendario — Calendario distribuido y planificador de grupos

Sistema de calendario compartido orientado a la coordinación entre múltiples
usuarios, con soporte para desconexión temporal, edición concurrente, distintos
niveles de privacidad y desacuerdos en la aceptación de reuniones.

> **Estado del proyecto:** diseño v3 congelado, implementación del hito 1 en
> curso. Ver [`docs/arquitectura.md`](docs/arquitectura.md).

## Motivación

Un calendario compartido no es solo un CRUD de eventos: introduce coordinación
distribuida real. Varios usuarios pueden modificar el mismo evento a la vez,
pueden estar desconectados y reconectar con cambios pendientes, y necesitan
reglas claras sobre qué información es visible para quién y cuándo una reunión
se considera realmente confirmada.

Este proyecto usa ese contexto para estudiar **consistencia, transacciones,
identidad y reconciliación**.

## Objetivo general

Construir un sistema que gestione:

- Eventos personales.
- Invitaciones y reuniones de grupo.
- Flujos de aceptación y rechazo.
- Consultas de disponibilidad.
- Detección de conflictos de horario.
- Controles de privacidad.
- Funcionamiento durante desconexiones temporales.
- Sincronización y reconciliación posterior.

## Alcance funcional

### Eventos personales

Alta, edición, borrado y consulta de eventos propios, con horarios, duración,
zona horaria y metadatos básicos.

### Invitaciones y reuniones de grupo

Un organizador propone una reunión a uno o varios participantes. Cada invitado
gestiona su respuesta de forma independiente.

### Aceptar / rechazar

Flujo explícito de respuesta por invitado, con estados diferenciados y
visibilidad del agregado para el organizador.

### Consulta de disponibilidad

Preguntar por huecos libres de un conjunto de usuarios sin exponer el detalle
de sus eventos (ver *Privacidad*).

### Conflictos de horario

Detección de solapamientos tanto en el calendario personal como al proponer una
reunión de grupo.

### Privacidad

Niveles de visibilidad por evento y por consulta: desde "ocupado" sin detalle
hasta información completa, según permisos.

### Desconexión y sincronización

El cliente debe poder operar sin conexión y reconciliar sus cambios al volver a
estar en línea, resolviendo conflictos de forma explícita.

### Propuesta vs. invitación aceptada vs. evento confirmado

Distinción de estados clave del dominio:

| Estado                              | Significado                                                         |
| ----------------------------------- | ------------------------------------------------------------------- |
| **Propuesta**                       | El organizador plantea fecha/hora; no compromete a nadie.           |
| **Invitación aceptada**             | Un invitado concreto ha aceptado participar.                        |
| **Evento de grupo confirmado**      | La reunión se considera cerrada según la regla acordada.            |

La regla de confirmación (unanimidad, quórum, mínimos, etc.) es una decisión de
diseño pendiente.

## Retos técnicos previstos

- **Consistencia:** ¿qué garantía se ofrece ante ediciones concurrentes?
- **Reconciliación:** estrategia de resolución de conflictos tras reconexión
  (last-write-wins, versiones vectoriales, merges por campo…).
- **Identidad:** usuarios, sesiones y autoría de cada cambio.
- **Privacidad:** separar "cuándo está ocupado" de "qué está haciendo".
- **Interoperabilidad:** representación de eventos independiente del transporte.

## Referencias de comparación

- **Google Calendar** — comparación funcional: calendarios compartidos,
  invitaciones y modelo de permisos.
- **Nextcloud Calendar / CalDAV** — comparación abierta: sincronización e
  interoperabilidad entre clientes.
- **iCalendar (RFC 5545)** — estándar de representación de eventos,
  independiente de la arquitectura distribuida.

## Stack

**Go en las dos puntas.**

| Capa | Tecnología |
| --- | --- |
| Servidor | Go + [`hashicorp/raft`](https://github.com/hashicorp/raft) + `raft-boltdb/v2` |
| Cliente | Go (store SQLite, outbox, firma) + [Wails v3](https://wails.io) |
| Protocolo | `pkg/protocol/`: sobre, JCS ([RFC 8785](https://www.rfc-editor.org/rfc/rfc8785)), Ed25519 |
| Persistencia cliente | SQLite (`modernc.org/sqlite`, sin cgo) |
| Verificación | `anishathalye/porcupine` (linealizabilidad) |

El motivo es que el protocolo se implementa **una sola vez**: `pkg/protocol/` lo
importan el nodo y el cliente, así que la firma y la serialización canónica no
pueden divergir entre lenguajes.

Requisitos:

| Parte | Requisito |
| --- | --- |
| Servidor | Go 1.22+ |
| Cliente | Go 1.24+ (Wails v3), Node 22 para compilar el frontend |
| Cliente en Linux | `libwebkit2gtk-4.1-dev` |

## Estructura del repositorio

```text
.
├── README.md
├── go.mod                    # módulo raíz: servidor + protocolo
├── docs/
│   └── arquitectura.md       # diseño v3 (fuente de verdad)
├── pkg/
│   └── protocol/             # sobre, JCS, Ed25519 — compartido
├── server/
│   ├── cmd/sdcald/           # binario del nodo
│   └── internal/
│       ├── raftnode/         # hashicorp/raft + boltdb
│       ├── fsm/              # máquina de estados replicada
│       └── api/              # HTTP, reenvío al líder
├── client/                   # app Wails (ver client/README.md)
└── deploy/
    ├── Dockerfile            # build multi-etapa
    ├── Dockerfile.runtime    # con binario precompilado
    ├── docker-compose.yml    # 3 nodos
    └── demo.sh               # demo de pérdida de líder
```

## Cómo ejecutarlo

```bash
# Tests: protocolo, FSM y un clúster de 3 nodos en memoria que pierde al líder
go test ./...

# Nodo suelto en local
go run ./server/cmd/sdcald -id=n1 -raft-advertise=127.0.0.1:7000 \
  -http-advertise=127.0.0.1:8080 -bootstrap \
  -peers=n1@127.0.0.1:7000@127.0.0.1:8080

# Clúster de 3 nodos + demo de fallo del líder
./deploy/demo.sh
```

Si el daemon de Docker no puede bajar `golang:1.22-alpine`, la demo compila el
binario en local y usa una imagen mínima:

```bash
SDCAL_DOCKERFILE=deploy/Dockerfile.runtime ./deploy/demo.sh
```

Salida esperada de la demo:

```text
==> matando al líder (n2)
==> esperando un líder nuevo con 2 de 3 nodos
  8081 -> demo=despues-del-fallo  {"id":"n1","state":"Leader","applied_index":9}
  8082 -> caído
  8083 -> demo=despues-del-fallo  {"id":"n3","state":"Follower","applied_index":9}
```

## Próximos pasos

1. ~~Decidir stack~~ → Go + Wails.
2. ~~Hito 1: 3 nodos Raft, FSM de eco, reenvío al líder y pérdida del líder.~~
3. Hito 2: FSM v1 (`REQUEST`/`REPLY`) con dedupe y el test `replay ≡ snapshot`.
4. Hito 3: vectores dorados de JCS y Ed25519.
5. Hito 4: cliente Wails con calendario personal y outbox offline.
6. Hito 5: invitaciones, reglas de confirmación y deadline.
7. Hito 6: `BUSY_UPDATE` y disponibilidad.
8. Hito 7: simulador y `porcupine`.

El diseño detallado —modelo de datos, protocolo, invariantes, API y reglas de
confirmación— está en [`docs/arquitectura.md`](docs/arquitectura.md).
