#!/usr/bin/env bash
# Entrypoint del nodo head de Ray.
set -euo pipefail

exec ray start --head \
  --port=6379 \
  --ray-client-server-port=10001 \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=8265 \
  --resources '{"sdcal-node-head": 1}' \
  --block
