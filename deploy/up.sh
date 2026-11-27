#!/usr/bin/env bash
# Levanta el despliegue de referencia: 1 head, 5 workers y 3 APIs sin estado.
#
# Variables de entorno:
#   SDCAL_NETWORK   nombre de la red Docker (sdcal)
#   SDCAL_IMAGE     prefijo de las imágenes (sdcal-ray:local)
#   SDCAL_WORKERS   número de workers (5)
#   SDCAL_APIS      número de APIs (3)
#   SDCAL_API_TOKENS JSON de usuario a token Bearer
#   SDCAL_AUTH_SECRET clave compartida para las sesiones de usuario
#   SDCAL_AUTH_RATE_LIMIT intentos de registro/login por IP y ventana (30)
#   SDCAL_AUTH_RATE_WINDOW_S ventana del límite de autenticación en segundos (60)
#   SDCAL_SKIP_BUILD=1 reutiliza las imágenes ya construidas
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NETWORK="${SDCAL_NETWORK:-sdcal}"
IMAGE="${SDCAL_IMAGE:-sdcal-ray:local}"
WORKERS="${SDCAL_WORKERS:-5}"
APIS="${SDCAL_APIS:-3}"

case "$WORKERS" in
  ''|*[!0-9]*)
    printf 'FALLO: SDCAL_WORKERS debe ser un entero positivo\n' >&2
    exit 1
    ;;
esac
case "$APIS" in
  ''|*[!0-9]*)
    printf 'FALLO: SDCAL_APIS debe ser un entero positivo\n' >&2
    exit 1
    ;;
esac
if [ "$WORKERS" -lt 5 ]; then
  printf 'FALLO: SDCAL_WORKERS debe ser al menos 5 (5 réplicas Raft con placement estricto)\n' >&2
  exit 1
fi
if [ "$APIS" -lt 1 ]; then
  printf 'FALLO: SDCAL_APIS debe ser al menos 1\n' >&2
  exit 1
fi

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

UP_SUCCEEDED=0
cleanup_partial() {
  if [ "$UP_SUCCEEDED" = "1" ]; then
    return
  fi
  printf '\nFALLO: limpiando despliegue parcial...\n' >&2
  docker rm -f $(seq -f 'sdcal-api-%g' 1 "$APIS") >/dev/null 2>&1 || true
  docker rm -f $(seq -f 'ray-worker-%g' 1 "$WORKERS") >/dev/null 2>&1 || true
  docker rm -f ray-head >/dev/null 2>&1 || true
}
trap cleanup_partial EXIT

if [ -z "${SDCAL_AUTH_SECRET:-}" ] || [ "${#SDCAL_AUTH_SECRET}" -lt 32 ]; then
  printf 'FALLO: configura SDCAL_AUTH_SECRET con al menos 32 caracteres\n' >&2
  exit 1
fi

log "creando red"
docker network create "$NETWORK" >/dev/null 2>&1 || true

# Limpia contenedores de una ejecución anterior que quedó a medias.
docker rm -f $(seq -f 'sdcal-api-%g' 1 "$APIS") >/dev/null 2>&1 || true
docker rm -f $(seq -f 'ray-worker-%g' 1 "$WORKERS") >/dev/null 2>&1 || true
docker rm -f ray-head >/dev/null 2>&1 || true

if [ "${SDCAL_SKIP_BUILD:-0}" != "1" ]; then
  # Con un wheelhouse local la imagen se construye sin red: `pip download`
  # deja las ruedas en deploy/wheelhouse y la imagen las usa con --no-index.
  PIP_ARGS="--timeout 60 --retries 10"
  if compgen -G "$ROOT/deploy/wheelhouse/*.whl" >/dev/null; then
    PIP_ARGS="--no-index"
  fi
  log "construyendo imágenes por tipo de nodo"
  docker build --build-arg PIP_ARGS="$PIP_ARGS" \
    -f "$ROOT/deploy/Dockerfile" -t "$IMAGE-head" "$ROOT"
  docker build --build-arg PIP_ARGS="$PIP_ARGS" \
    -f "$ROOT/deploy/Dockerfile.worker" -t "$IMAGE-worker" "$ROOT"
  docker build --build-arg PIP_ARGS="$PIP_ARGS" \
    -f "$ROOT/deploy/Dockerfile.api" -t "$IMAGE-api" "$ROOT"
fi

log "levantando Ray head"
docker run -d --restart unless-stopped --name ray-head --network "$NETWORK" \
  -p 6379:6379 -p 8265:8265 \
  -v sdcal-gcs-data:/app/data/gcs \
  "$IMAGE-head" >/dev/null

sleep 5

log "levantando $WORKERS workers"
for index in $(seq 1 "$WORKERS"); do
  docker run -d --restart unless-stopped \
    --name "ray-worker-$index" --network "$NETWORK" \
    -e SDCAL_NODE_ID="$index" \
    -v "sdcal-data-$index:/app/data" \
    "$IMAGE-worker" >/dev/null
done

sleep 5

log "levantando $APIS APIs sin estado"
for index in $(seq 1 "$APIS"); do
  port=$((8080 + index))
  docker run -d --restart unless-stopped \
    --name "sdcal-api-$index" --network "$NETWORK" \
    -e SDCAL_API_TOKENS="${SDCAL_API_TOKENS:-}" \
    -e SDCAL_AUTH_SECRET="$SDCAL_AUTH_SECRET" \
    -e SDCAL_AUTH_RATE_LIMIT="${SDCAL_AUTH_RATE_LIMIT:-}" \
    -e SDCAL_AUTH_RATE_WINDOW_S="${SDCAL_AUTH_RATE_WINDOW_S:-}" \
    -p "$port:8080" "$IMAGE-api" >/dev/null
  # El proxy de Ray Client asigna un puerto por conexión; escalonar el
  # arranque evita la carrera entre las tres APIs.
  if [ "$index" -lt "$APIS" ]; then
    sleep 2
  fi
done

log "esperando APIs listas con líder y quórum"
for index in $(seq 1 "$APIS"); do
  port=$((8080 + index))
  healthy=0
  for _ in $(seq 1 90); do
    if curl -fsS "http://localhost:$port/ready" >/dev/null 2>&1; then
      healthy=1
      break
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' "sdcal-api-$index" 2>/dev/null)" != "true" ]; then
      docker logs "sdcal-api-$index" 2>&1 | tail -40 >&2
      exit 1
    fi
    sleep 1
  done
  if [ "$healthy" != "1" ]; then
    docker logs "sdcal-api-$index" 2>&1 | tail -40 >&2
    exit 1
  fi
done

log "verificando estado de los contenedores"
for container in ray-head; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]; then
    docker logs "$container" 2>&1 | tail -40 >&2
    exit 1
  fi
done
for index in $(seq 1 "$WORKERS"); do
  container="ray-worker-$index"
  if [ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]; then
    docker logs "$container" 2>&1 | tail -40 >&2
    exit 1
  fi
done
for index in $(seq 1 "$APIS"); do
  container="sdcal-api-$index"
  if [ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]; then
    docker logs "$container" 2>&1 | tail -40 >&2
    exit 1
  fi
done

log "despliegue listo: APIs en 8081-$((8080 + APIS))"
UP_SUCCEEDED=1
