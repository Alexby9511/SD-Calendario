#!/usr/bin/env bash
# Entrypoint del nodo head de Ray.
set -euo pipefail

# Evita los handlers atfork de gRPC en el proxy multihilo de Ray Client.
export GRPC_ENABLE_FORK_SUPPORT=0
export RAY_gcs_storage=rocksdb
export RAY_gcs_storage_path=/app/data/gcs
export RAY_gcs_rpc_server_reconnect_timeout_s=120

exec ray start --head \
  --port=6379 \
  --ray-client-server-port=10001 \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=8265 \
  --resources '{"sdcal-node-head": 1}' \
  --block
