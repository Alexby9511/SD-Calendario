#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${SDCAL_API_TOKENS:-}" ]; then
  SDCAL_API_TOKENS="$(docker exec sdcal-api-1 printenv SDCAL_API_TOKENS 2>/dev/null || true)"
  export SDCAL_API_TOKENS
fi

if [ -z "$SDCAL_API_TOKENS" ]; then
  printf 'FALLO: configura SDCAL_API_TOKENS como JSON de usuario a token\n' >&2
  exit 1
fi

"$ROOT/deploy/up.sh"
python3 "$ROOT/deploy/verify_cluster.py"
