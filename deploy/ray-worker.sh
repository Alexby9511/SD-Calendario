#!/usr/bin/env bash
# Entrypoint de un worker de Ray.
#
# Cada worker declara un recurso único (`sdcal-node-<id>`). El planificador de
# Raft usa ese recurso para fijar una réplica por nodo: el recurso sobrevive al
# reinicio del contenedor, a diferencia del NodeID de Ray.
set -euo pipefail

node_id="${SDCAL_NODE_ID:-worker}"
address="${RAY_ADDRESS:-ray-head:6379}"

exec ray start --address="$address" \
  --resources "{\"sdcal-node-${node_id}\": 1}" \
  --block
