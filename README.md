# SD Calendario — prototipo distribuido sobre Ray

Calendario compartido para eventos personales, invitaciones, aceptación,
rechazo, disponibilidad, conflictos y reconciliación offline.

El estado autoritativo se replica con Raft en actores de Ray. El cliente
prototipo conserva comandos pendientes en una outbox SQLite para reintentar
la sincronización después de una desconexión.

## Stack

| Capa | Tecnología |
| --- | --- |
| Servidor | Python + Ray |
| API | FastAPI |
| Protocolo | JCS/RFC 8785 |
| Identidad API | Registro de usuario/contraseña y tokens Bearer |
| Estado | FSM determinista replicada por cinco actores Raft |
| Cliente prototipo | Python + SQLite outbox |
| Despliegue | Dockerfiles individuales + `docker run` |

Requisitos: Python 3.10+, Docker opcional y Ray 2.60+.

Al abrir la aplicación se muestra el registro o inicio de sesión; no se puede
usar el calendario sin una sesión válida. Cada usuario elige un identificador
único (3–32 caracteres) y una contraseña de al menos 8 caracteres. Las cuentas
y sus hashes se guardan en el estado replicado por Raft. Configura una clave
secreta compartida por todas las APIs fuera del repositorio:

```bash
export SDCAL_AUTH_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"
```

Las rutas de datos requieren un token Bearer emitido tras iniciar sesión. La
configuración antigua `SDCAL_API_TOKENS` se mantiene opcionalmente para clientes
heredados; esos tokens no habilitan el registro de cuentas.

La identidad autorizada procede exclusivamente del token Bearer. Aunque el
protocolo transporta `key_id` y `signature` y `Envelope.verify()` está
disponible para uso explícito, la admisión actual no verifica firmas Ed25519 ni
exige que `key_id` corresponda a `sender_id`; esos campos no autentican ni
autorizan comandos. No uses este prototipo como sustituto de esa verificación
en un despliegue que dependa de firmas de dispositivo.

`visibility` admite solo `private` y `public`. La disponibilidad responde solo
`available` o `busy`; una ruta privada muestra sus detalles únicamente al
organizador y a los participantes. `/health` comprueba el proceso y `/ready`
comprueba autenticación, líder y quórum.

## Ejecutar localmente

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
pytest
SDCAL_AUTH_SECRET='<secreto-compartido-de-al-menos-32-bytes>' \
  python -m server.cmd.sdcald --no-ray --port 8080
```

Con Ray en el equipo:

```bash
SDCAL_AUTH_SECRET='<secreto-compartido-de-al-menos-32-bytes>' \
  python -m server.cmd.sdcald --port 8080
```

## API

- `POST /v1/commands`
- `POST /v1/command` — compatibilidad con el hito anterior
- `POST /v1/auth/register`
- `POST /v1/auth/login`
- `GET /v1/auth/me`
- `GET /v1/status`
- `GET /v1/kv/{key}`
- `GET /v1/availability`
- `GET /v1/conflicts`
- `GET /v1/sync`
- `GET /health`
- `GET /ready`

## Despliegue Docker manual

```bash
export SDCAL_AUTH_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"
export SDCAL_DEMO_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SDCAL_API_TOKENS="$(python -c 'import json,os; print(json.dumps({"alice": os.environ["SDCAL_DEMO_TOKEN"]}))')"
./deploy/demo.sh
```

El procedimiento completo está en
[`deploy/despliegue-manual.md`](deploy/despliegue-manual.md). La demo levanta
un head, cinco workers y tres APIs; las escrituras se confirman mediante el
líder Raft y el mismo `msg_id` reenviado queda deduplicado por la FSM.

Las lecturas de disponibilidad y conflictos se ejecutan como Ray Tasks sobre
snapshots inmutables. `/v1/sync` omite los payloads `BUSY_UPDATE`; las páginas
no copian la proyección completa y el cliente la solicita una sola vez al
terminar de leerlas. El cursor se conserva por despliegue y usuario.

Las reuniones de grupo usan por defecto su hora de inicio como fecha límite
para la votación. El líder Raft emite evaluaciones internas y marca como
`EXPIRED` las propuestas que sigan abiertas al vencer esa fecha.

El head usa GCS RocksDB persistente para recuperarse de reinicios de su
contenedor. La recuperación de GCS con RocksDB es experimental en Ray; el
despliegue local no garantiza tolerancia a la pérdida física del host Docker.
