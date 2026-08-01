#!/bin/bash
# Build the host agent as a small static Linux/amd64 binary for Databricks nodes.
# Databricks classic compute runs on x86-64 Linux, so we target that regardless
# of your build machine.
set -euo pipefail

OUT="vrahad-agent"

echo "building ${OUT} (linux/amd64, static)..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o "$OUT" .

sha256sum "$OUT" | tee "${OUT}.sha256"
ls -lh "$OUT"

echo
echo "next steps:"
echo "  1. upload ${OUT} to your bucket / UC volume"
echo "  2. set AGENT_BINARY_URL (and optionally AGENT_SHA256 from ${OUT}.sha256) in init-scripts/install-agent.sh"
echo "  3. attach the init script to your cluster and set AGENT_ENDPOINT / AGENT_TOKEN in the cluster's Spark env"
