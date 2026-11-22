#!/usr/bin/env bash
# Detiene el despliegue de referencia y borra sus volúmenes.
set -euo pipefail

NETWORK="${SDCAL_NETWORK:-sdcal}"
WORKERS="${SDCAL_WORKERS:-5}"
APIS="${SDCAL_APIS:-3}"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

log "deteniendo contenedores"
docker rm -f $(seq -f 'sdcal-api-%g' 1 "$APIS") >/dev/null 2>&1 || true
docker rm -f $(seq -f 'ray-worker-%g' 1 "$WORKERS") >/dev/null 2>&1 || true
docker rm -f ray-head >/dev/null 2>&1 || true

log "borrando volúmenes de datos"
docker volume rm $(seq -f 'sdcal-data-%g' 1 "$WORKERS") >/dev/null 2>&1 || true

log "borrando red"
docker network rm "$NETWORK" >/dev/null 2>&1 || true
