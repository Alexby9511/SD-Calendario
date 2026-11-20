#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NETWORK="${SDCAL_NETWORK:-sdcal}"
IMAGE="${SDCAL_IMAGE:-sdcal-ray:local}"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
cleanup() {
  docker rm -f sdcal-api ray-worker-2 ray-worker-1 ray-head >/dev/null 2>&1 || true
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

log "levantando workers"
docker run -d --name ray-worker-1 --network "$NETWORK" "$IMAGE-worker" >/dev/null
docker run -d --name ray-worker-2 --network "$NETWORK" "$IMAGE-worker" >/dev/null

sleep 5

log "levantando API"
docker run -d --name sdcal-api --network "$NETWORK" -p 8081:8080 \
  "$IMAGE-api" >/dev/null

log "esperando API"
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8081/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://localhost:8081/health >/dev/null

COMMAND='{"deployment_id":"dep1","group_id":"team","msg_id":"01J00000000000000000000010","type":"REQUEST","uid":"alice:01J00000000000000000000011","sender_id":"alice","key_id":"key1","sequence":1,"seen_sequence":0,"client_sent_at":"2026-11-20T00:00:00Z","payload":{"title":"Demo Ray","kind":"group","participants":["bob"]}}'

log "creando propuesta"
curl -fsS -X POST http://localhost:8081/v1/commands \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "reintentando el mismo msg_id"
curl -fsS -X POST http://localhost:8081/v1/commands \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "estado"
curl -fsS http://localhost:8081/v1/status
