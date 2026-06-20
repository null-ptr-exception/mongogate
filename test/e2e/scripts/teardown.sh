#!/usr/bin/env bash
# Tears down the mongogate E2E kind cluster.
set -euo pipefail
CLUSTER_NAME="mongogate-e2e"

if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo "cluster $CLUSTER_NAME not found, nothing to do"
fi
