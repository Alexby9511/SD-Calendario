#!/usr/bin/env bash
# Verifica todos los pares de fallo del despliegue de referencia (9 contenedores).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKERS="${SDCAL_WORKERS:-5}"
APIS="${SDCAL_APIS:-3}"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFALLO: %s\033[0m\n' "$*" >&2; exit 1; }

[ "$WORKERS" = "5" ] || fail "la prueba requiere exactamente 5 workers"
[ "$APIS" = "3" ] || fail "la prueba requiere exactamente 3 APIs"
[ -n "${SDCAL_API_TOKENS:-}" ] || fail "configura SDCAL_API_TOKENS"

ALICE_TOKEN="$(python3 -c 'import json,os; value=json.loads(os.environ["SDCAL_API_TOKENS"])["alice"]; print(value[0] if isinstance(value,list) else value)')"
[ -n "$ALICE_TOKEN" ] || fail "no se encontró un token para alice"
AUTH_HEADER="Authorization: Bearer $ALICE_TOKEN"
RUN_ID="pair-$(date +%s)-$$"

CONTAINERS=(ray-head)
for index in $(seq 1 "$WORKERS"); do
  CONTAINERS+=("ray-worker-$index")
done
for index in $(seq 1 "$APIS"); do
  CONTAINERS+=("sdcal-api-$index")
done

restore_all() {
  for container in "${CONTAINERS[@]}"; do
    docker start "$container" >/dev/null 2>&1 || true
  done
}
trap restore_all EXIT

status_json() {
  curl -fsS -H "$AUTH_HEADER" "$API_URL/v1/status"
}
replica_count() {
  status_json | python3 -c 'import json,sys; print(json.load(sys.stdin).get("alive_replicas", 0))' 2>/dev/null
}
leader_count() {
  status_json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(r.get("role") == "leader" for r in d.get("replicas", {}).values()))' 2>/dev/null
}
have_replicas() { [ "$(replica_count)" = "$1" ]; }
have_single_leader() { [ "$(leader_count)" = "1" ]; }
head_running() {
  [ "$(docker inspect -f '{{.State.Running}}' ray-head 2>/dev/null)" = "true" ]
}
ready() {
  curl -fsS "$API_URL/ready" >/dev/null
}
wait_for() {
  local timeout="$1"
  shift
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}
choose_api() {
  local first="$1"
  local second="$2"
  for index in $(seq 1 "$APIS"); do
    local candidate="sdcal-api-$index"
    if [ "$candidate" != "$first" ] && [ "$candidate" != "$second" ]; then
      API_URL="http://localhost:$((8080 + index))"
      return 0
    fi
  done
  return 1
}
expected_replicas() {
  local first="$1"
  local second="$2"
  local down=0
  [[ "$first" == ray-worker-* ]] && down=$((down + 1))
  [[ "$second" == ray-worker-* ]] && down=$((down + 1))
  echo $((WORKERS - down))
}
verify_event_sync() {
  local uid="$1"
  local response
  response="$(curl -fsS -H "$AUTH_HEADER" \
    "$API_URL/v1/sync?since=0&limit=1000")" || return 1
  printf '%s' "$response" | python3 -c \
    'import json,sys; d=json.load(sys.stdin); uid=sys.argv[1]; assert uid in d.get("snapshot", {}).get("events", {}), "event missing from authorized sync snapshot"' \
    "$uid"
}
write_and_check() {
  local pair_number="$1"
  local uid="alice:${RUN_ID}-${pair_number}"
  local msg_id="${RUN_ID}-${pair_number}"
  local sent_at
  local start
  local end
  local response
  sent_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  # Every pair needs its own window: the events stay in the state machine, and
  # reusing one window would make every event after the first collide with the
  # organizer's own calendar (409 organizer_schedule_conflict).
  start="$(date -u -d "@$(( $(date -u -d '2026-11-22T00:00:00Z' +%s) + pair_number * 86400 ))" '+%Y-%m-%dT%H:%M:%SZ')"
  end="$(date -u -d "@$(( $(date -u -d '2026-11-22T00:00:00Z' +%s) + pair_number * 86400 + 3600 ))" '+%Y-%m-%dT%H:%M:%SZ')"
  response="$(curl -fsS -X POST "$API_URL/v1/commands" \
    -H "$AUTH_HEADER" \
    -H 'Content-Type: application/json' \
    -d "{\"deployment_id\":\"dep1\",\"group_id\":\"team\",\"msg_id\":\"$msg_id\",\"type\":\"REQUEST\",\"uid\":\"$uid\",\"sender_id\":\"alice\",\"key_id\":\"key1\",\"sequence\":1,\"seen_sequence\":0,\"client_sent_at\":\"$sent_at\",\"payload\":{\"title\":\"Prueba de resiliencia\",\"start\":\"$start\",\"end\":\"$end\",\"visibility\":\"private\"}}")" \
    || return 1
  printf '%s' "$response" | python3 -c \
    'import json,sys; d=json.load(sys.stdin); assert d.get("code") == 200, d' \
    || return 1
  verify_event_sync "$uid"
}

log "levantando el despliegue de referencia (1 head, 5 workers, 3 APIs)"
"$ROOT/deploy/up.sh"

API_URL="http://localhost:8081"
wait_for 120 ready || fail "la API inicial no está lista"
wait_for 120 have_replicas 5 || fail "no se levantaron las 5 réplicas"
wait_for 120 have_single_leader || fail "no se eligió un único líder"

pair_number=0
for ((first_index = 0; first_index < ${#CONTAINERS[@]}; first_index++)); do
  for ((second_index = first_index + 1; second_index < ${#CONTAINERS[@]}; second_index++)); do
    first="${CONTAINERS[$first_index]}"
    second="${CONTAINERS[$second_index]}"
    pair_number=$((pair_number + 1))
    log "par $pair_number/36: caída de $first y $second"

    choose_api "$first" "$second" || fail "no queda una API disponible"

    for container in "$first" "$second"; do
      if [ "$container" = "ray-head" ]; then
        docker kill "$container" >/dev/null
      else
        docker stop "$container" >/dev/null
      fi
    done

    if [[ "$first" == ray-head || "$second" == ray-head ]]; then
      wait_for 120 head_running \
        || fail "Ray head no se recuperó automáticamente"
    fi

    expected="$(expected_replicas "$first" "$second")"
    wait_for 120 ready || fail "el servicio no recuperó readiness para $first + $second"
    wait_for 120 have_replicas "$expected" \
      || fail "cantidad inesperada de réplicas para $first + $second"
    wait_for 120 have_single_leader \
      || fail "no hay exactamente un líder para $first + $second"
    write_and_check "$pair_number" \
      || fail "la escritura o sincronización falló para $first + $second"

    for container in "$first" "$second"; do
      if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ]; then
        docker start "$container" >/dev/null
      fi
    done
    API_URL="http://localhost:8081"
    wait_for 180 ready || fail "readiness no volvió después de recuperar $first + $second"
    wait_for 180 have_replicas "$WORKERS" \
      || fail "no se recuperaron las 5 réplicas después de $first + $second"
    wait_for 180 have_single_leader \
      || fail "se perdió el líder después de recuperar $first + $second"
  done
done

log "completadas las 36 combinaciones; el despliegue queda ejecutándose"
