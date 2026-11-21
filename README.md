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

## Estructura del proyecto

```text
SD-Calendario/
├── README.md                         # Documentación principal
├── pyproject.toml                    # Paquete, dependencias y configuración de pytest
├── pkg/
│   └── protocol/
│       ├── canonical.py               # Serialización JCS/RFC 8785
│       ├── codes.py                  # Códigos de respuesta del protocolo
│       └── envelope.py               # Envelope, validación y firma Ed25519
├── server/
│   ├── cmd/
│   │   └── sdcald.py                 # Entrada CLI y arranque de uvicorn/Ray
│   ├── ray_app/
│   │   ├── api.py                    # API FastAPI
│   │   ├── coordinator.py            # Coordinador local y actor Ray
│   │   └── fsm.py                    # Máquina de estados determinista
│   └── tests/
│       ├── test_api.py               # Pruebas de la API
│       ├── test_dedupe.py            # Pruebas de deduplicación
│       ├── test_fsm.py               # Pruebas de dominio y sincronización
│       └── test_ray_cluster.py       # Prueba opcional del actor Ray
├── client/
│   ├── README.md                     # Uso del cliente local-first
│   └── outbox.py                     # Cola persistente SQLite y cursor de sincronización
├── docs/
│   ├── arquitectura.md               # Diseño funcional y protocolo
│   ├── arquitectura-ray.md           # Adaptación al runtime Ray
│   └── mapa-dependencias.md          # Responsabilidades entre Ray y la aplicación
└── deploy/
    ├── Dockerfile                    # Imagen head de Ray
    ├── Dockerfile.worker             # Imagen worker de Ray
    ├── Dockerfile.api                # Imagen de la API FastAPI
    ├── demo.sh                       # Demo Docker automatizada
    └── despliegue-manual.md          # Procedimiento de despliegue manual
```

## TODO

- [ ] Persistir snapshots y el log determinista en almacenamiento externo.
- [ ] Recuperar el actor coordinador desde snapshot/log después de reinicios.
- [ ] Añadir registro y rotación de claves públicas para verificar Ed25519.
- [ ] Aplicar autorización por rol en todos los comandos y consultas.
- [ ] Completar el frontend local-first y sustituir el prototipo por la integración Wails.
- [ ] Ampliar las pruebas de la API, Ray y recuperación ante fallos.
- [ ] Estabilizar la topología de producción y definir cuotas/estrategias de failover.
- [ ] Añadir observabilidad: métricas, trazas y almacenamiento de auditoría.

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
