#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NETWORK="${SDCAL_NETWORK:-sdcal}"
IMAGE="${SDCAL_IMAGE:-sdcal-ray:local}"
WORKERS="${SDCAL_WORKERS:-5}"
APIS="${SDCAL_APIS:-3}"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
cleanup() {
  docker rm -f $(seq -f 'sdcal-api-%g' 1 "$APIS") >/dev/null 2>&1 || true
  docker rm -f $(seq -f 'ray-worker-%g' 1 "$WORKERS") >/dev/null 2>&1 || true
  docker rm -f ray-head >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "creando red"
docker network create "$NETWORK" >/dev/null

log "construyendo imágenes por tipo de nodo"
docker build -f "$ROOT/deploy/Dockerfile" -t "$IMAGE-head" "$ROOT"
docker build -f "$ROOT/deploy/Dockerfile.worker" -t "$IMAGE-worker" "$ROOT"
docker build -f "$ROOT/deploy/Dockerfile.api" -t "$IMAGE-api" "$ROOT"

log "levantando Ray head"
docker run -d --name ray-head --network "$NETWORK" \
  -p 6379:6379 -p 8265:8265 \
  "$IMAGE-head" >/dev/null

sleep 5

log "levantando $WORKERS workers"
for index in $(seq 1 "$WORKERS"); do
  docker run -d --name "ray-worker-$index" --network "$NETWORK" \
    "$IMAGE-worker" >/dev/null
done

sleep 5

log "levantando $APIS APIs sin estado"
for index in $(seq 1 "$APIS"); do
  port=$((8080 + index))
  docker run -d --name "sdcal-api-$index" --network "$NETWORK" \
    -p "$port:8080" "$IMAGE-api" >/dev/null
done

log "esperando APIs"
for index in $(seq 1 "$APIS"); do
  port=$((8080 + index))
  for _ in $(seq 1 60); do
    if curl -fsS "http://localhost:$port/health" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  curl -fsS "http://localhost:$port/health" >/dev/null
done

COMMAND='{"deployment_id":"dep1","group_id":"team","msg_id":"01J00000000000000000000010","type":"REQUEST","uid":"alice:01J00000000000000000000011","sender_id":"alice","key_id":"key1","sequence":1,"seen_sequence":0,"client_sent_at":"2026-11-20T00:00:00Z","payload":{"title":"Demo Ray","kind":"group","participants":["bob"]}}'

log "creando propuesta a través de la API 1"
curl -fsS -X POST http://localhost:8081/v1/commands \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "reintentando el mismo msg_id a través de la API 2"
curl -fsS -X POST http://localhost:8082/v1/commands \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "estado desde la API 3"
curl -fsS http://localhost:8083/v1/status
