# SD Calendario — despliegue distribuido sobre Ray

Calendario compartido para eventos personales, invitaciones, aceptación,
rechazo, disponibilidad, conflictos y reconciliación offline.

El estado autoritativo se replica con Raft en actores de Ray. La API es
FastAPI y la interfaz web es React. Este README explica cómo levantar el
despliegue de referencia completo con Docker en una sola computadora.

## Arquitectura del despliegue

El despliegue de referencia levanta **9 contenedores en una misma máquina
Docker**:

| Contenedor | Función |
| --- | --- |
| `ray-head` | Coordinador de Ray, Ray Client en `10001`, dashboard en `8265` y volumen `sdcal-gcs-data` |
| `ray-worker-1` … `ray-worker-5` | Una réplica Raft por worker, con su SQLite en `sdcal-data-N` |
| `sdcal-api-1` … `sdcal-api-3` | APIs FastAPI sin estado en `8081`, `8082` y `8083` |

Hay **5 réplicas Raft**, una por worker, con **quórum 3/5**. El líder es la
única autoridad de escritura; los seguidores replican el log y aplican la misma
FSM. Las APIs son intercambiables: todas se conectan al Ray Client del head y
envían las escrituras al líder.

Ray solo aporta el runtime distribuido. El consenso Raft está implementado en
`server/ray_app/consensus/` y no depende de frameworks externos.

Puntos importantes:

- El despliegue es distribuido **en contenedores**, pero todo corre en la misma
  computadora. No es un despliegue multi-host.
- El head de Ray es un **SPOF aceptado**: si cae, el clúster deja de coordinar.
- Cada réplica persiste término, voto, log y snapshot en su SQLite local.
- Los volúmenes `sdcal-data-1` … `sdcal-data-5` y `sdcal-gcs-data` conservan el
  estado entre reinicios de contenedores.

## Requisitos

- Docker Engine + CLI, con permiso para ejecutar Docker sin `sudo`.
- Git.
- Node.js 18+ solo si vas a usar la interfaz web.
- Puertos libres: `8081`, `8082`, `8083`, `6379` y `8265`.
- Conexión a Internet para el primer build de las imágenes, salvo que
  `deploy/wheelhouse/` contenga las wheels necesarias.
- Varios GB de RAM disponibles. No es un despliegue ligero.

No hace falta crear un entorno virtual de Python ni instalar las dependencias
en el host para este despliegue: las imágenes Docker ya incluyen la aplicación.

## Secretos

Para levantar el clúster solo necesitas el secreto de autenticación:

```bash
export SDCAL_AUTH_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"
```

`SDCAL_AUTH_SECRET`:

- debe tener al menos 32 bytes;
- debe ser el mismo en las tres APIs;
- debe conservarse entre reinicios. Si cambia, los tokens Bearer ya emitidos
  dejan de ser válidos.

No escribas este secreto en el repositorio.

La demo y los clientes heredados usan además un token Bearer estático:

```bash
export SDCAL_DEMO_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SDCAL_API_TOKENS="$(python3 -c 'import json,os; print(json.dumps({"alice": os.environ["SDCAL_DEMO_TOKEN"]}))')"
```

`SDCAL_API_TOKENS` es el mecanismo legado. No hace falta para levantar el
clúster ni para usar la interfaz web; solo se usa en la demo y en scripts de
verificación o caos.

## Levantar el clúster desde cero

```bash
git clone <url-del-repositorio> "SD Calendario"
cd "SD Calendario"

export SDCAL_AUTH_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"

./deploy/up.sh
```

`up.sh` hace lo siguiente:

1. Crea la red Docker `sdcal`.
2. Construye las imágenes `sdcal-ray:local-head`, `sdcal-ray:local-worker` y
   `sdcal-ray:local-api`.
3. Levanta `ray-head`.
4. Levanta los 5 workers.
5. Levanta las 3 APIs en `8081`, `8082` y `8083`.
6. Espera a que `/ready` responda en las tres APIs.

Si las imágenes ya están construidas, puedes reutilizarlas:

```bash
SDCAL_SKIP_BUILD=1 ./deploy/up.sh
```

`up.sh` elimina y recrea los contenedores de una ejecución anterior, pero
**conserva los volúmenes**. Para borrar también los datos hay que usar
`down.sh`.

No uses `demo.sh` para dejar el clúster levantado: es una prueba temporal que
borra todo al terminar. Su comportamiento se explica más abajo.

## Verificar que está funcionando

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8081/ready
```

`/health` comprueba que el proceso responde. `/ready` exige autenticación
configurada, líder y quórum; si falta alguno, devuelve `503`.

Para obtener un token sin usar la interfaz web:

```bash
curl -fsS -X POST http://localhost:8081/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"alice","password":"password123"}'
```

La respuesta incluye `access_token`. Úsalo para consultar el estado:

```bash
curl -fsS -H "Authorization: Bearer <access_token>" \
  http://localhost:8081/v1/status
```

El estado debe mostrar:

- `middleware: "ray"`;
- `alive_replicas: 5`;
- `quorum_available: true`;
- un `leader_id` válido.

Las tres APIs son equivalentes; puedes repetir la comprobación contra `8082` y
`8083`.

### Rutas principales

- `POST /v1/auth/register`
- `POST /v1/auth/login`
- `GET /v1/auth/me`
- `POST /v1/commands` (alias `POST /v1/command`)
- `GET /v1/status`
- `GET /v1/kv/{key}` (`*` devuelve el snapshot visible)
- `GET /v1/availability`
- `GET /v1/conflicts`
- `GET /v1/sync`
- `GET /health`
- `GET /ready`

Todas las rutas `/v1/*` requieren `Authorization: Bearer <token>`. `/health`
y `/ready` son las únicas sin autenticación.

## Interfaz web

Con el clúster levantado, en otra terminal:

```bash
cd ui
npm install
npm run dev
```

Abre `http://localhost:5173`, crea una cuenta y entra. El proxy de Vite envía
`/v1` y `/health` a `http://localhost:8081`.

Para desarrollo usa `npm run dev`. `npm run preview` no tiene proxy configurado
para `/v1`, así que las llamadas a la API no llegarían al backend.

## Operación diaria

Ver logs:

```bash
docker logs -f ray-head
docker logs -f sdcal-api-1
```

Parar los contenedores conservando los datos:

```bash
docker stop sdcal-api-{1..3} ray-worker-{1..5} ray-head
```

Volver a levantar el clúster:

```bash
export SDCAL_AUTH_SECRET="<el-mismo-secreto>"
SDCAL_SKIP_BUILD=1 ./deploy/up.sh
```

Borrar contenedores, red y volúmenes:

```bash
./deploy/down.sh
```

`down.sh` **elimina los datos replicados**. Úsalo solo cuando quieras empezar
de cero.

## Demo end-to-end

La demo es una comprobación rápida de que el camino completo funciona. Necesita
el secreto de autenticación y un token estático para `alice`:

```bash
export SDCAL_AUTH_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"
export SDCAL_DEMO_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SDCAL_API_TOKENS="$(python3 -c 'import json,os; print(json.dumps({"alice": os.environ["SDCAL_DEMO_TOKEN"]}))')"

./deploy/demo.sh
```

Qué hace exactamente `demo.sh`:

1. Comprueba que exista `SDCAL_DEMO_TOKEN`.
2. Ejecuta `up.sh`, que construye y levanta el clúster completo.
3. Envía un comando `REQUEST` con un `msg_id` fijo a
   `http://localhost:8081/v1/commands`, autenticado como `alice`.
4. Reenvía **el mismo comando y el mismo `msg_id`** a
   `http://localhost:8082/v1/commands`. La FSM lo deduplica y no crea un
   segundo evento.
5. Consulta `/v1/status` desde `http://localhost:8083`.
6. Al salir, ejecuta `down.sh`, que elimina contenedores, red y volúmenes.

> `demo.sh` es una prueba temporal. No lo uses si quieres conservar el clúster
> o los datos.

## Problemas frecuentes

- **Puerto ocupado**: comprueba `8081`–`8083`, `6379` y `8265` con
  `ss -ltnp` o `docker ps`.
- **`permission denied` al ejecutar Docker**: tu usuario debe pertenecer al
  grupo `docker` o debes ejecutar con los permisos adecuados.
- **El primer build falla por red**: las imágenes instalan dependencias desde
  PyPI. Sin Internet, hay que poblar `deploy/wheelhouse/` antes de construir.
- **`/ready` devuelve `503`**: todavía no hay líder o quórum. Espera unos
  segundos y revisa `docker logs ray-head` y los logs de las APIs.
- **Menos de 5 workers**: la imagen de API usa `--raft-replicas=5` y
  `--raft-placement=strict`. Un clúster nuevo necesita 5 workers para colocar
  las 5 réplicas.
- **El secreto cambió entre APIs**: los tokens dejan de validar. Usa el mismo
  `SDCAL_AUTH_SECRET` en todas las APIs y consérvalo entre reinicios.
- **Quedan contenedores de una ejecución anterior**: `up.sh` los elimina y
  recrea, pero los volúmenes se conservan. Usa `down.sh` si quieres borrarlos.
- **La UI no llega a la API**: usa `npm run dev`, que aplica el proxy de Vite a
  `localhost:8081`.

## Referencias

- Procedimiento manual detallado: [`deploy/despliegue-manual.md`](deploy/despliegue-manual.md).
- Scripts de despliegue, demo y caos: `deploy/`.
- Interfaz web: `ui/`.
