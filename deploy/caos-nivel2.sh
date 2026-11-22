#!/usr/bin/env bash
# Caos de nivel 2 sobre el despliegue de referencia.
#
# Caen a la vez 2 procesos de API y 2 nodos de Ray (4 réplicas en total, 3/5
# vivos). El servicio debe seguir confirmando comandos, no debe perder los
# comandos ya confirmados y, al volver los nodos, las réplicas deben recuperar
# su log local y volver al quórum completo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKERS="${SDCAL_WORKERS:-5}"
APIS="${SDCAL_APIS:-3}"
API1="http://localhost:8081"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFALLO: %s\033[0m\n' "$*" >&2; exit 1; }

if [ -z "${SDCAL_API_TOKENS:-}" ]; then
  fail "configura SDCAL_API_TOKENS antes de ejecutar la prueba"
fi
ALICE_TOKEN="$(python3 -c 'import json,os; value=json.loads(os.environ["SDCAL_API_TOKENS"])["alice"]; print(value[0] if isinstance(value,list) else value)')"
AUTH_HEADER="Authorization: Bearer $ALICE_TOKEN"
RUN_ID="chaos-$(date +%s)-$$"

cleanup() {
  docker start ray-head >/dev/null 2>&1 || true
  for index in $(seq 1 "$WORKERS"); do
    docker start "ray-worker-$index" >/dev/null 2>&1 || true
  done
  for index in $(seq 1 "$APIS"); do
    docker start "sdcal-api-$index" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

status_json() {
  curl -sS -H "$AUTH_HEADER" "$API1/v1/status" 2>/dev/null || echo '{}'
}

json_eval() {
  python3 -c "import json,sys; data=json.load(sys.stdin); print($1)" 2>/dev/null || echo ""
}

replica_count() { status_json | json_eval 'len(data["replicas"])'; }
leader_count() {
  status_json | json_eval 'len([r for r in data["replicas"].values() if r["role"] == "leader"])'
}
applied_min() {
  status_json | json_eval 'min([r["last_applied"] for r in data["replicas"].values()] or [0])'
}
placement_keys() {
  status_json | json_eval '",".join(sorted(r.get("placement_key") or "-" for r in data["replicas"].values()))'
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

have_leader() { [ "$(leader_count)" = "1" ]; }
have_three_replicas() { [ "$(replica_count)" = "3" ]; }
have_five_replicas() { [ "$(replica_count)" = "5" ]; }
applied_at_least_two() { [ "$(applied_min)" -ge 2 ] 2>/dev/null; }

post_command() {
  local msg_id="$1"
  curl -sS -o /tmp/sdcal-chaos-response.json -w '%{http_code}' \
    -X POST "$API1/v1/commands" \
    -H "$AUTH_HEADER" \
    -H 'Content-Type: application/json' \
    -d "{\"deployment_id\":\"dep1\",\"group_id\":\"team\",\"msg_id\":\"$msg_id\",\"type\":\"REQUEST\",\"uid\":\"alice:$msg_id\",\"sender_id\":\"alice\",\"key_id\":\"key1\",\"sequence\":1,\"seen_sequence\":0,\"client_sent_at\":\"2026-11-22T00:00:00Z\",\"payload\":{\"title\":\"Caos nivel 2\"}}"
}

log "levantando el despliegue (5 workers, 3 APIs)"
"$ROOT/deploy/up.sh"

wait_for 60 have_five_replicas || fail "el clúster no tiene 5 réplicas"
wait_for 60 have_leader || fail "el clúster no tiene líder"
log "estado inicial: 5 réplicas, 1 líder, placement=$(placement_keys)"

MSG_A="$RUN_ID-a"
MSG_B="$RUN_ID-b"
log "confirmando el comando A antes del caos"
code="$(post_command "$MSG_A")"
[ "$code" = "200" ] || fail "el comando A no se confirmó (HTTP $code)"
grep -q '"deduplicated": *false' /tmp/sdcal-chaos-response.json \
  || fail "el comando A no se aplicó por primera vez"

log "parando 2 APIs (2 y 3) y 2 nodos (4 y 5)"
docker stop "sdcal-api-2" "sdcal-api-3" >/dev/null
docker stop "ray-worker-4" "ray-worker-5" >/dev/null

wait_for 90 have_three_replicas || fail "no quedan exactamente 3 réplicas"
wait_for 90 have_leader || fail "no hay líder con 3 réplicas"
[ "$(leader_count)" = "1" ] || fail "split-brain: hay más de un líder"

log "confirmando el comando B con 3/5 réplicas"
code="$(post_command "$MSG_B")"
[ "$code" = "200" ] || fail "el comando B no se confirmó sin quórum (HTTP $code)"

log "comprobando que el comando A no se perdió"
found="$(curl -sS -H "$AUTH_HEADER" "$API1/v1/kv/alice:$MSG_A" | json_eval 'data["found"]')"
[ "$found" = "True" ] || fail "el comando A desapareció del estado"

log "volviendo a levantar los 2 nodos"
docker start "ray-worker-4" "ray-worker-5" >/dev/null

wait_for 120 have_five_replicas || fail "las réplicas no se reincorporaron"
wait_for 120 applied_at_least_two || fail "las réplicas recuperadas no están al día"
[ "$(leader_count)" = "1" ] || fail "split-brain tras la recuperación"
log "estado recuperado: 5 réplicas, 1 líder, last_applied mínimo=$(applied_min)"

log "volviendo a levantar las 2 APIs"
docker start "sdcal-api-2" "sdcal-api-3" >/dev/null
for index in 2 3; do
  port=$((8080 + index))
  wait_for 90 bash -c "curl -fsS http://localhost:$port/health >/dev/null" \
    || fail "la API $index no vuelve a estar sana"
done

log "reintentando el comando A en otra API (dedupe)"
retry_code="$(curl -sS -o /tmp/sdcal-chaos-retry.json -w '%{http_code}' \
  -X POST "http://localhost:8082/v1/commands" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d "{\"deployment_id\":\"dep1\",\"group_id\":\"team\",\"msg_id\":\"$MSG_A\",\"type\":\"REQUEST\",\"uid\":\"alice:$MSG_A\",\"sender_id\":\"alice\",\"key_id\":\"key1\",\"sequence\":1,\"seen_sequence\":0,\"client_sent_at\":\"2026-11-22T00:00:00Z\",\"payload\":{\"title\":\"Caos nivel 2\"}}")"
[ "$retry_code" = "200" ] || fail "el reintento devolvió HTTP $retry_code"
grep -q '"deduplicated": *true' /tmp/sdcal-chaos-retry.json \
  || fail "el reintento no se deduplicó"

log "caos de nivel 2 completado"
