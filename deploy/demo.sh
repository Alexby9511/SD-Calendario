#!/usr/bin/env bash
#
# Hito 1: tres nodos Raft, escritura reenviada al líder y pérdida del líder.
#
#   ./deploy/demo.sh
#
# Con Docker sin acceso al registro (no puede bajar golang:1.22-alpine):
#   SDCAL_DOCKERFILE=deploy/Dockerfile.runtime ./deploy/demo.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/docker-compose.yml")
PORTS=(8081 8082 8083)
GO="${GO:-go}"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

status() { curl -fsS --max-time 2 "http://localhost:$1/v1/status"; }

leader_port() {
  local port
  for port in "${PORTS[@]}"; do
    if status "$port" 2>/dev/null | jq -e '.state == "Leader"' >/dev/null 2>&1; then
      echo "$port"
      return 0
    fi
  done
  return 1
}

wait_leader() {
  local timeout=${1:-90} start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if leader_port >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "no leader after ${timeout}s"
}

apply_set() {
  local port=$1 key=$2 value=$3
  curl -fsS --max-time 10 -X POST "http://localhost:$port/v1/command" \
    -H 'Content-Type: application/json' \
    -d "{\"op\":\"set\",\"key\":\"$key\",\"value\":\"$value\"}"
}

read_value() {
  curl -fsS --max-time 5 "http://localhost:$1/v1/kv/$2" | jq -r '.value'
}

if [[ -n "${SDCAL_DOCKERFILE:-}" ]]; then
  log "compilando binario linux (la imagen builder no está disponible)"
  (cd "$ROOT" && CGO_ENABLED=0 GOOS=linux "$GO" build -trimpath -o "$ROOT/sdcald" ./server/cmd/sdcald)
fi

cleanup() {
  "${COMPOSE[@]}" down >/dev/null 2>&1 || true
  rm -f "$ROOT/sdcald"
}
trap cleanup EXIT

log "levantando el clúster"
"${COMPOSE[@]}" up -d --build

log "esperando líder"
wait_leader 120

log "estado del clúster"
for port in "${PORTS[@]}"; do
  status "$port" | jq -c '{id, state, leader_id, applied_index}'
done

leader=$(leader_port)
follower=""
for port in "${PORTS[@]}"; do
  [[ "$port" != "$leader" ]] && follower="$port" && break
done

log "escribiendo a través de un follower (puerto $follower) para probar el reenvío"
apply_set "$follower" demo antes | jq -c .
sleep 1
for port in "${PORTS[@]}"; do
  printf '  %s -> %s\n' "$port" "$(read_value "$port" demo)"
done

leader_id=$(status "$leader" | jq -r '.id')
log "matando al líder ($leader_id)"
docker kill "sdcal-$leader_id" >/dev/null

log "esperando un líder nuevo con 2 de 3 nodos"
wait_leader 60

log "escribiendo después del fallo"
for port in "${PORTS[@]}"; do
  if status "$port" >/dev/null 2>&1; then
    apply_set "$port" demo despues-del-fallo | jq -c .
    break
  fi
done
sleep 1

log "estado final"
for port in "${PORTS[@]}"; do
  if status "$port" >/dev/null 2>&1; then
    printf '  %s -> demo=%s  %s\n' \
      "$port" \
      "$(read_value "$port" demo)" \
      "$(status "$port" | jq -c '{id, state, applied_index}')"
  else
    printf '  %s -> caído\n' "$port"
  fi
done

log "OK: el clúster siguió aceptando escrituras con un nodo menos"
