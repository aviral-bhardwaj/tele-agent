// vrahad-agent is a lightweight per-node host-metrics collector for Databricks
// clusters. It runs on every node (driver and workers), samples CPU / memory /
// disk / network / filesystem from /proc, and ships batches to an ingest
// endpoint over HTTPS.
//
// Design priorities, in order:
//  1. Never harm the host. Hard resource ceilings, bounded memory, drop-on-full.
//  2. Survive network loss. Buffer locally, retry with backoff, keep sampling.
//  3. Stay cheap. Single sampling goroutine, GOMAXPROCS(1), tiny payloads.
//
// It is deliberately dependency-free (stdlib only) so the binary is small and
// static, and so an init script can curl one file and run it.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	Endpoint    string        // where to POST batches
	Token       string        // bearer token for the ingest gateway
	Interval    time.Duration // sampling period
	FlushEvery  time.Duration // max time a batch waits before being sent
	BufferMax   int           // max samples held in memory (backpressure bound)
	BatchMax    int           // max samples per POST
	MemLimitMiB int           // soft process memory ceiling (GOMEMLIMIT)

	ClusterID   string // DB_CLUSTER_ID
	ClusterName string // DB_CLUSTER_NAME
	WorkspaceID string // DB_WORKSPACE_ID (if present)
	Role        string // "driver" | "worker"
	NodeID      string // hostname — unique within the cluster
}

// envOr returns the env var or a fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func loadConfig() config {
	interval, _ := strconv.Atoi(envOr("AGENT_INTERVAL_SEC", "15"))
	flush, _ := strconv.Atoi(envOr("AGENT_FLUSH_SEC", "30"))
	bufMax, _ := strconv.Atoi(envOr("AGENT_BUFFER_MAX", "2000"))
	batchMax, _ := strconv.Atoi(envOr("AGENT_BATCH_MAX", "120"))
	memLimit, _ := strconv.Atoi(envOr("AGENT_MEM_LIMIT_MIB", "64"))

	role := "worker"
	if strings.EqualFold(os.Getenv("DB_IS_DRIVER"), "TRUE") {
		role = "driver"
	}

	host, _ := os.Hostname()

	return config{
		Endpoint:    envOr("AGENT_ENDPOINT", ""),
		Token:       os.Getenv("AGENT_TOKEN"),
		Interval:    time.Duration(interval) * time.Second,
		FlushEvery:  time.Duration(flush) * time.Second,
		BufferMax:   bufMax,
		BatchMax:    batchMax,
		MemLimitMiB: memLimit,
		ClusterID:   os.Getenv("DB_CLUSTER_ID"),
		ClusterName: os.Getenv("DB_CLUSTER_NAME"),
		WorkspaceID: os.Getenv("DB_WORKSPACE_ID"),
		Role:        role,
		NodeID:      host,
	}
}

// envelope is what actually goes over the wire: node identity plus a batch of
// samples. The backend keys on (cluster_id, node_id) and appends.
type envelope struct {
	ClusterID   string   `json:"cluster_id"`
	ClusterName string   `json:"cluster_name"`
	WorkspaceID string   `json:"workspace_id"`
	Role        string   `json:"role"`
	NodeID      string   `json:"node_id"`
	Samples     []sample `json:"samples"`
}

// ringBuffer is a bounded FIFO. When full it drops the OLDEST sample to make
// room for the newest — a deliberate choice: under sustained backpressure we'd
// rather keep recent data and lose stale data than block the sampler or grow
// memory without bound. Every drop is counted so the gap is observable.
type ringBuffer struct {
	mu      sync.Mutex
	buf     []sample
	max     int
	dropped uint64
}

func newRing(max int) *ringBuffer {
	return &ringBuffer{buf: make([]sample, 0, max), max: max}
}

func (r *ringBuffer) push(s sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) >= r.max {
		r.buf = r.buf[1:] // drop oldest
		r.dropped++
	}
	r.buf = append(r.buf, s)
}

// drain removes up to n samples and returns them.
func (r *ringBuffer) drain(n int) []sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		return nil
	}
	if n > len(r.buf) {
		n = len(r.buf)
	}
	out := make([]sample, n)
	copy(out, r.buf[:n])
	r.buf = r.buf[n:]
	return out
}

// requeue puts samples back at the FRONT after a failed send, so ordering is
// preserved and a transient network blip doesn't lose data.
func (r *ringBuffer) requeue(s []sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	combined := append(s, r.buf...)
	if len(combined) > r.max {
		over := len(combined) - r.max
		combined = combined[over:] // if requeue overflows, still drop oldest
		r.dropped += uint64(over)
	}
	r.buf = combined
}

func (r *ringBuffer) stats() (queued int, dropped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.buf), r.dropped
}

// sampler reads /proc on a fixed cadence and pushes computed samples into the
// ring. It holds the previous raw snapshot to compute rates. This is the only
// goroutine that touches /proc, so no locking is needed around the snapshots.
func sampler(ctx context.Context, cfg config, ring *ringBuffer) {
	prevCPU, okCPU := readCPU()
	prevIO := readIO()
	prevT := time.Now()

	// Prime with one immediate read after the first interval; rates need two points.
	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			curCPU, ok := readCPU()
			curIO := readIO()
			secs := now.Sub(prevT).Seconds()

			var cpuPct float64
			if okCPU && ok {
				cpuPct = cpuPercentBetween(prevCPU, curCPU)
			}
			memPct, memTotal := readMem()

			s := sample{
				TS:               now.UnixMilli(),
				CPUPercent:       round1(cpuPct),
				MemUsedPercent:   round1(memPct),
				MemTotalBytes:    memTotal,
				Load1:            readLoad1(),
				DiskReadBps:      round1(perSec(prevIO.diskReadBytes, curIO.diskReadBytes, secs)),
				DiskWriteBps:     round1(perSec(prevIO.diskWriteBytes, curIO.diskWriteBytes, secs)),
				NetRxBps:         round1(perSec(prevIO.netRxBytes, curIO.netRxBytes, secs)),
				NetTxBps:         round1(perSec(prevIO.netTxBytes, curIO.netTxBytes, secs)),
				RootFSUsedPct:    round1(fsUsedPct("/")),
				LocalDiskUsedPct: round1(fsUsedPct("/local_disk0")),
			}
			ring.push(s)

			prevCPU, okCPU = curCPU, ok
			prevIO = curIO
			prevT = now
		}
	}
}

// sender drains the ring and POSTs batches. On failure it requeues and backs
// off, so the network being down never blocks or crashes the sampler.
func sender(ctx context.Context, cfg config, ring *ringBuffer, client *http.Client) {
	tick := time.NewTicker(cfg.FlushEvery)
	defer tick.Stop()

	backoff := time.Second
	const maxBackoff = 60 * time.Second

	// flush sends one batch using the supplied context. During normal operation
	// that's the main ctx; during shutdown it's a fresh short-lived context, so
	// a final flush can still complete after the main ctx has been cancelled.
	flush := func(sendCtx context.Context) {
		batch := ring.drain(cfg.BatchMax)
		if len(batch) == 0 {
			return
		}
		if err := post(sendCtx, cfg, client, batch); err != nil {
			ring.requeue(batch)
			log.Printf("send failed (%d samples requeued): %v; backing off %s", len(batch), err, backoff)
			select {
			case <-time.After(backoff):
			case <-sendCtx.Done():
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			return
		}
		backoff = time.Second // reset on success
	}

	for {
		select {
		case <-ctx.Done():
			// Best-effort final flush on a fresh context — the main ctx is
			// already cancelled, so reusing it would fail instantly.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			flush(shutdownCtx)
			cancel()
			queued, dropped := ring.stats()
			log.Printf("shutdown: %d samples still queued, %d dropped over lifetime", queued, dropped)
			return
		case <-tick.C:
			flush(ctx)
		}
	}
}

func post(ctx context.Context, cfg config, client *http.Client, batch []sample) error {
	env := envelope{
		ClusterID:   cfg.ClusterID,
		ClusterName: cfg.ClusterName,
		WorkspaceID: cfg.WorkspaceID,
		Role:        cfg.Role,
		NodeID:      cfg.NodeID,
		Samples:     batch,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ingest returned %s", resp.Status)
	}
	return nil
}

func round1(f float64) float64 {
	if f < 0 {
		return f // preserve -1 sentinel from fsUsedPct
	}
	return float64(int64(f*10+0.5)) / 10
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetPrefix("[vrahad-agent] ")

	cfg := loadConfig()
	if cfg.Endpoint == "" {
		log.Fatal("AGENT_ENDPOINT is required")
	}

	// Ceiling 1: CPU. One OS thread for scheduling — this is a background
	// collector, not a workload. It should be invisible in the node's CPU graph.
	runtime.GOMAXPROCS(1)

	// Ceiling 2: memory. A hard GOMEMLIMIT makes the GC work harder rather than
	// let the heap grow, so a backend outage can never turn the agent into the
	// reason a worker OOMs. The ring buffer bound backs this up.
	debug.SetMemoryLimit(int64(cfg.MemLimitMiB) * 1024 * 1024)

	log.Printf("starting: cluster=%s node=%s role=%s -> %s (interval=%s, buffer=%d)",
		cfg.ClusterID, cfg.NodeID, cfg.Role, cfg.Endpoint, cfg.Interval, cfg.BufferMax)

	ring := newRing(cfg.BufferMax)
	client := &http.Client{Timeout: 15 * time.Second}

	// Cancel everything cleanly on SIGTERM/SIGINT — Databricks sends SIGTERM
	// when a cluster scales in or terminates, and we want a final flush.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); sampler(ctx, cfg, ring) }()
	go func() { defer wg.Done(); sender(ctx, cfg, ring, client) }()
	wg.Wait()

	log.Print("stopped")
}
