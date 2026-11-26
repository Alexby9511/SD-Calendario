#!/usr/bin/env bash
# Demo del despliegue de referencia: propuesta, reintento con el mismo msg_id
# y estado del clúster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${SDCAL_AUTH_SECRET:-}" ] || [ "${#SDCAL_AUTH_SECRET}" -lt 32 ]; then
  printf 'FALLO: configura SDCAL_AUTH_SECRET con al menos 32 caracteres\n' >&2
  exit 1
fi

if [ -z "${SDCAL_DEMO_TOKEN:-}" ] || [ -z "${SDCAL_API_TOKENS:-}" ]; then
  printf 'FALLO: configura SDCAL_DEMO_TOKEN y SDCAL_API_TOKENS para alice\n' >&2
  exit 1
fi

if ! python3 -c '
import json, os, sys
value = json.loads(os.environ["SDCAL_API_TOKENS"])["alice"]
tokens = [value] if isinstance(value, str) else value
sys.exit(0 if os.environ["SDCAL_DEMO_TOKEN"] in tokens else 1)
'; then
  printf 'FALLO: SDCAL_DEMO_TOKEN no es un token de alice en SDCAL_API_TOKENS\n' >&2
  exit 1
fi

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

cleanup() { "$ROOT/deploy/down.sh" >/dev/null 2>&1 || true; }
trap cleanup EXIT

"$ROOT/deploy/up.sh"

COMMAND='{"deployment_id":"dep1","group_id":"team","msg_id":"01J00000000000000000000010","type":"REQUEST","uid":"alice:01J00000000000000000000011","sender_id":"alice","key_id":"key1","sequence":1,"seen_sequence":0,"client_sent_at":"2026-11-20T00:00:00Z","payload":{"title":"Demo Ray","kind":"group","participants":["bob"]}}'

log "creando propuesta a través de la API 1"
curl -fsS -X POST http://localhost:8081/v1/commands \
  -H "Authorization: Bearer $SDCAL_DEMO_TOKEN" \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "reintentando el mismo msg_id a través de la API 2"
curl -fsS -X POST http://localhost:8082/v1/commands \
  -H "Authorization: Bearer $SDCAL_DEMO_TOKEN" \
  -H 'Content-Type: application/json' -d "$COMMAND"

log "estado desde la API 3"
curl -fsS -H "Authorization: Bearer $SDCAL_DEMO_TOKEN" \
  http://localhost:8083/v1/status
