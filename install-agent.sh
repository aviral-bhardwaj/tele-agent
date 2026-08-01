#!/bin/bash
#
# Vrahad host-agent installer — Databricks cluster-scoped init script.
#
# Runs as root on EVERY node (driver and workers) during startup, before Spark
# comes up. It downloads the agent binary and launches it as a detached daemon
# that ships per-node host metrics to the ingest endpoint.
#
# ── Install ───────────────────────────────────────────────────────────────
# 1. Upload the binary to a location the cluster can read (UC volume, S3, or
#    the workspace files API). Set AGENT_BINARY_URL below to match.
# 2. Store this script in a UC volume or workspace path (DBFS-stored and legacy
#    global init scripts are deprecated) and attach it under
#    Cluster → Advanced → Init Scripts.
# 3. Provide endpoint + token through the cluster's Spark env config so they
#    arrive as env vars — reference a Databricks secret, do NOT hardcode:
#      AGENT_ENDPOINT=https://ingest.telemetria.ai/v1/host
#      AGENT_TOKEN={{secrets/telemetria/ingest_token}}
#
# ── The one rule ──────────────────────────────────────────────────────────
# This script MUST NOT abort cluster startup. An init script that exits non-zero
# fails the whole cluster. A monitoring agent that stops the customer from
# starting their cluster is worse than no monitoring at all, so every failure
# path here logs and exits 0. The cluster always boots; at worst it boots blind.

set -uo pipefail   # deliberately NOT -e: see "The one rule" above.

# ── Config ──────────────────────────────────────────────────────────────────
AGENT_BINARY_URL="${AGENT_BINARY_URL:-https://your-bucket.s3.amazonaws.com/vrahad-agent/vrahad-agent}"
AGENT_SHA256="${AGENT_SHA256:-}"                 # optional: pin the binary's checksum
INSTALL_DIR="/usr/local/bin"
BIN_PATH="${INSTALL_DIR}/vrahad-agent"
LOG_DIR="/databricks/driver/logs"                # picked up by Databricks log delivery
LOG_FILE="${LOG_DIR}/vrahad-agent.log"

log() { echo "[vrahad-init] $(date -u +%FT%TZ) $*"; }

mkdir -p "$LOG_DIR" 2>/dev/null || LOG_FILE="/tmp/vrahad-agent.log"

# ── Download, with retries ──────────────────────────────────────────────────
log "downloading agent from ${AGENT_BINARY_URL}"
if ! curl -fsSL --retry 5 --retry-delay 3 --max-time 60 \
        -o "${BIN_PATH}.tmp" "$AGENT_BINARY_URL"; then
    log "ERROR: could not download agent binary — cluster will start without monitoring"
    exit 0
fi

# ── Optional integrity check ────────────────────────────────────────────────
if [[ -n "$AGENT_SHA256" ]]; then
    actual="$(sha256sum "${BIN_PATH}.tmp" | awk '{print $1}')"
    if [[ "$actual" != "$AGENT_SHA256" ]]; then
        log "ERROR: checksum mismatch (want ${AGENT_SHA256}, got ${actual}) — refusing to run"
        rm -f "${BIN_PATH}.tmp"
        exit 0
    fi
    log "checksum verified"
fi

mv "${BIN_PATH}.tmp" "$BIN_PATH"
chmod 0755 "$BIN_PATH"

# ── Sanity check the endpoint ───────────────────────────────────────────────
if [[ -z "${AGENT_ENDPOINT:-}" ]]; then
    log "ERROR: AGENT_ENDPOINT not set in cluster env config — not launching agent"
    exit 0
fi

# ── Launch as a detached daemon ─────────────────────────────────────────────
# setsid + nohup so the agent survives the init script exiting and is not tied
# to the init script's process group. Databricks env vars (DB_CLUSTER_ID,
# DB_IS_DRIVER, ...) are already in the environment and the agent reads them.
#
# Runs on every node on purpose: host metrics from workers are the whole point —
# that's how you catch one hot worker or a /local_disk0 filling up before spill
# fails. (The SparkListener is the driver-only piece; this is not.)
log "launching agent on $([ "${DB_IS_DRIVER:-FALSE}" = "TRUE" ] && echo driver || echo worker) node ${DB_CLUSTER_ID:-unknown}"

setsid nohup "$BIN_PATH" >> "$LOG_FILE" 2>&1 &

log "agent launched (pid $!), logging to ${LOG_FILE}"
exit 0
