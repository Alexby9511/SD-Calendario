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
| Identidad API | Tokens Bearer por usuario |
| Estado | FSM determinista replicada por cinco actores Raft |
| Cliente prototipo | Python + SQLite outbox |
| Despliegue | Dockerfiles individuales + `docker run` |

Requisitos: Python 3.10+, Docker opcional y Ray 2.60+.

Las rutas de datos requieren un token Bearer. Configura tokens fuera del
repositorio como JSON de usuario a token (mínimo 32 caracteres):

```bash
export SDCAL_DEMO_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SDCAL_API_TOKENS="$(python -c 'import json,os; print(json.dumps({"alice": os.environ["SDCAL_DEMO_TOKEN"]}))')"
```

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
SDCAL_API_TOKENS='{"alice":"<token-de-32-caracteres-o-mas>"}' \
  python -m server.cmd.sdcald --no-ray --port 8080
```

Con Ray en el equipo:

```bash
SDCAL_API_TOKENS='{"alice":"<token-de-32-caracteres-o-mas>"}' \
  python -m server.cmd.sdcald --port 8080
```

## API

- `POST /v1/commands`
- `POST /v1/command` — compatibilidad con el hito anterior
- `GET /v1/status`
- `GET /v1/kv/{key}`
- `GET /v1/availability`
- `GET /v1/conflicts`
- `GET /v1/sync`
- `GET /health`
- `GET /ready`

## Despliegue Docker manual

```bash
export SDCAL_DEMO_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SDCAL_API_TOKENS="$(python -c 'import json,os; print(json.dumps({"alice": os.environ["SDCAL_DEMO_TOKEN"]}))')"
./deploy/demo.sh
```

El procedimiento completo está en
[`deploy/despliegue-manual.md`](deploy/despliegue-manual.md). La demo levanta
un head, cinco workers y tres APIs; las escrituras se confirman mediante el
líder Raft y el mismo `msg_id` reenviado queda deduplicado por la FSM.

Las lecturas de disponibilidad, conflictos y sincronización se ejecutan como
Ray Tasks sobre snapshots inmutables. `/v1/sync` omite los payloads
`BUSY_UPDATE`, manteniendo un cursor que permite al cliente reanudar la
reconciliación sin recibir ventanas de disponibilidad. La outbox SQLite
persiste comandos completos, reintenta con el mismo `msg_id` y guarda
atómicamente cada página reconciliada junto con su cursor.

El head usa GCS RocksDB persistente para recuperarse de reinicios de su
contenedor. La recuperación de GCS con RocksDB es experimental en Ray; el
despliegue local no garantiza tolerancia a la pérdida física del host Docker.
