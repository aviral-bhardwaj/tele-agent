# vrahad-agent

A lightweight per-node host-metrics collector for Databricks clusters. It runs
on every node, reads `/proc` on a fixed cadence, and ships batches of
CPU / memory / disk / network / filesystem metrics to an ingest endpoint over
HTTPS. Stdlib-only, compiles to a ~5 MB static binary, no dependencies.

This is the **host agent** channel of the telemetria observability design — the
node-level "is this cluster healthy and right-sized" signal. It is deliberately
separate from the SparkListener (job/stage/shuffle/skew) and the system-tables
(fleet cost) channels.

## What it collects

Every sample, per node:

| field | meaning |
|---|---|
| `cpu_percent` | aggregate CPU utilisation, 0–100 across all cores |
| `mem_used_percent` | memory in use (via `MemAvailable`, the honest number) |
| `mem_total_bytes` | total RAM |
| `load1` | 1-minute load average |
| `disk_read_bps` / `disk_write_bps` | physical disk throughput, bytes/sec |
| `net_rx_bps` / `net_tx_bps` | network throughput, bytes/sec (excl. loopback) |
| `root_fs_used_pct` | `/` fill % |
| `local_disk_used_pct` | `/local_disk0` fill % — the Spark spill target; `-1` if absent |

Each batch is tagged with `cluster_id`, `cluster_name`, `workspace_id`, `role`
(driver/worker) and `node_id`, all read from the Databricks environment.

## Why these design choices

The agent is privileged code on someone else's production node, so it is built
to be incapable of harming the host:

- **CPU ceiling** — `GOMAXPROCS(1)`. It's a background collector, not a workload.
- **Memory ceiling** — a hard `GOMEMLIMIT` (default 64 MiB) plus a bounded ring
  buffer. A backend outage can never turn the agent into an OOM cause.
- **Backpressure** — the ring buffer drops the *oldest* sample when full and
  counts every drop. It never blocks the sampler and never grows without bound.
- **Network loss tolerance** — failed sends requeue and back off exponentially;
  sampling continues regardless. A final best-effort flush runs on `SIGTERM`
  (which Databricks sends on scale-in / termination).
- **The init script never aborts cluster startup** — every failure path logs and
  exits 0. Monitoring must not be able to stop the thing it monitors.

## Build

```bash
./build.sh          # -> vrahad-agent (linux/amd64, static) + .sha256
```

## Configure

All configuration is via environment variables. On Databricks, the `DB_*` vars
are already present; you supply the rest through the cluster's Spark env config.

| env var | default | notes |
|---|---|---|
| `AGENT_ENDPOINT` | *(required)* | HTTPS URL to POST batches to |
| `AGENT_TOKEN` | — | bearer token; reference a Databricks secret |
| `AGENT_INTERVAL_SEC` | `15` | sampling period |
| `AGENT_FLUSH_SEC` | `30` | max time a batch waits before sending |
| `AGENT_BUFFER_MAX` | `2000` | max samples held in memory |
| `AGENT_BATCH_MAX` | `120` | max samples per POST |
| `AGENT_MEM_LIMIT_MIB` | `64` | soft process memory ceiling |
| `DB_CLUSTER_ID`, `DB_CLUSTER_NAME`, `DB_WORKSPACE_ID`, `DB_IS_DRIVER` | — | set by Databricks |

## Deploy

1. Upload `vrahad-agent` to a bucket / UC volume the cluster can read.
2. In `init-scripts/install-agent.sh`, set `AGENT_BINARY_URL` (and optionally
   `AGENT_SHA256` from the build output).
3. Store the init script in a UC volume or workspace path and attach it under
   **Cluster → Advanced → Init Scripts**.
4. In the cluster's **Spark → Environment variables**, set:
   ```
   AGENT_ENDPOINT=https://ingest.telemetria.ai/v1/host
   AGENT_TOKEN={{secrets/telemetria/ingest_token}}
   ```

## The wire format

The agent POSTs JSON:

```json
{
  "cluster_id": "0801-...-abcd",
  "cluster_name": "telemetria-dev",
  "workspace_id": "...",
  "role": "worker",
  "node_id": "10-0-1-23",
  "samples": [
    { "ts": 1785595635191, "cpu_percent": 87.4, "mem_used_percent": 62.1,
      "mem_total_bytes": 68719476736, "load1": 5.9,
      "disk_read_bps": 0, "disk_write_bps": 524288000,
      "net_rx_bps": 1048576, "net_tx_bps": 2097152,
      "root_fs_used_pct": 41.2, "local_disk_used_pct": 88.7 }
  ]
}
```

Your ingest backend keys on `(cluster_id, node_id)` and appends.

## Important limits

- **Classic compute only.** Serverless has no init scripts and no host access —
  there your only channels are system tables and the REST APIs. Do not bet the
  product on this agent alone.
- **Init scripts are increasingly restricted** by cluster policies. For selling
  into other orgs' workspaces, treat the system-tables channel as the baseline
  and this agent as an opt-in upgrade.
- **Linux/amd64 only** — matches Databricks nodes; the `/proc` collectors are
  Linux-specific by design.
