# SD Calendario — prototipo distribuido sobre Ray

Calendario compartido para eventos personales, invitaciones, aceptación,
rechazo, disponibilidad, conflictos y reconciliación offline.

Esta versión sustituye Raft por **Ray** en la Fase 1. El diseño adaptado está
en [`docs/arquitectura-ray.md`](docs/arquitectura-ray.md) y las responsabilidades
entre Ray y la aplicación están en
[`docs/mapa-dependencias.md`](docs/mapa-dependencias.md).

## Stack

| Capa | Tecnología |
| --- | --- |
| Servidor | Python + Ray |
| API | FastAPI |
| Protocolo | JCS/RFC 8785 + Ed25519 |
| Estado | FSM determinista en el actor coordinador |
| Cliente prototipo | Python + SQLite outbox |
| Despliegue | Dockerfiles individuales + `docker run` |

Requisitos: Python 3.10+, Docker opcional y Ray 2.9+.

## Ejecutar localmente

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
pytest
python -m server.cmd.sdcald --no-ray --port 8080
```

Con Ray en el equipo:

```bash
python -m server.cmd.sdcald --port 8080
```

## API

- `POST /v1/commands`
- `POST /v1/command` — compatibilidad con el hito anterior
- `GET /v1/status`
- `GET /v1/kv/{key}`
- `GET /health`

## Despliegue Docker manual

```bash
./deploy/demo.sh
```

El procedimiento completo está en
[`deploy/despliegue-manual.md`](deploy/despliegue-manual.md). La demo levanta
manualmente un head, dos workers y una API; el actor coordinador recibe RPCs
desde Ray y el mismo `msg_id` reenviado queda deduplicado por la FSM.
