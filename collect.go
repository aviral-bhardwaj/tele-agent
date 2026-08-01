package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// rawCPU is a point-in-time snapshot of the aggregate CPU counters from
// /proc/stat. Utilization is a rate, so we always compute it from the delta
// between two snapshots rather than from a single reading.
type rawCPU struct {
	idle  uint64 // idle + iowait
	total uint64 // sum of all fields
}

// rawIO holds cumulative counters that only make sense as deltas: bytes moved
// across disk and network since boot.
type rawIO struct {
	diskReadBytes  uint64
	diskWriteBytes uint64
	netRxBytes     uint64
	netTxBytes     uint64
}

// sample is one fully-computed observation ready to ship. Everything here is
// either an instantaneous gauge (percentages, load) or a per-second rate
// derived from two raw snapshots.
type sample struct {
	TS               int64   `json:"ts"`                  // unix millis
	CPUPercent       float64 `json:"cpu_percent"`         // 0-100 across all cores
	MemUsedPercent   float64 `json:"mem_used_percent"`    // 0-100
	MemTotalBytes    uint64  `json:"mem_total_bytes"`     //
	Load1            float64 `json:"load1"`               // 1-min load average
	DiskReadBps      float64 `json:"disk_read_bps"`       // bytes/sec
	DiskWriteBps     float64 `json:"disk_write_bps"`      // bytes/sec
	NetRxBps         float64 `json:"net_rx_bps"`          // bytes/sec
	NetTxBps         float64 `json:"net_tx_bps"`          // bytes/sec
	RootFSUsedPct    float64 `json:"root_fs_used_pct"`    // "/" fill %
	LocalDiskUsedPct float64 `json:"local_disk_used_pct"` // "/local_disk0" fill % (spill target)
}

// readCPU parses the aggregate "cpu" line of /proc/stat.
// Fields: user nice system idle iowait irq softirq steal guest guest_nice.
// idle-time = idle + iowait; total = sum of all counters.
func readCPU() (rawCPU, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return rawCPU{}, false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return rawCPU{}, false
	}
	fields := strings.Fields(sc.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return rawCPU{}, false
	}

	var total, idle uint64
	for i := 1; i < len(fields); i++ {
		v, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			continue
		}
		total += v
		if i == 4 || i == 5 { // idle, iowait
			idle += v
		}
	}
	return rawCPU{idle: idle, total: total}, true
}

// cpuPercentBetween turns two /proc/stat snapshots into a utilization %.
func cpuPercentBetween(prev, cur rawCPU) float64 {
	dTotal := float64(cur.total - prev.total)
	dIdle := float64(cur.idle - prev.idle)
	if dTotal <= 0 {
		return 0
	}
	busy := (dTotal - dIdle) / dTotal * 100
	if busy < 0 {
		busy = 0
	}
	return busy
}

// readMem parses MemTotal and MemAvailable (kB) from /proc/meminfo.
// MemAvailable is the kernel's own estimate of reclaimable memory and is the
// honest number for "how full is this box" — far better than MemFree.
func readMem() (usedPct float64, totalBytes uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()

	var memTotal, memAvail uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseUint(fields[1], 10, 64) // value is in kB
		switch fields[0] {
		case "MemTotal:":
			memTotal = v * 1024
		case "MemAvailable:":
			memAvail = v * 1024
		}
	}
	if memTotal == 0 {
		return 0, 0
	}
	used := memTotal - memAvail
	return float64(used) / float64(memTotal) * 100, memTotal
}

// readLoad1 returns the 1-minute load average from /proc/loadavg.
func readLoad1() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return v
}

const sectorBytes = 512 // Linux reports disk IO in 512-byte sectors regardless of physical sector size.

// readIO sums disk sectors and network bytes across real devices.
// Disk: /proc/diskstats — we skip loop/ram/dm virtual devices so a busy
// container overlay doesn't masquerade as physical IO.
// Net:  /proc/net/dev — we skip loopback.
func readIO() rawIO {
	var io rawIO

	if f, err := os.Open("/proc/diskstats"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) < 10 {
				continue
			}
			name := fields[2]
			if strings.HasPrefix(name, "loop") ||
				strings.HasPrefix(name, "ram") ||
				strings.HasPrefix(name, "dm-") {
				continue
			}
			// field[5] = sectors read, field[9] = sectors written
			sr, _ := strconv.ParseUint(fields[5], 10, 64)
			sw, _ := strconv.ParseUint(fields[9], 10, 64)
			io.diskReadBytes += sr * sectorBytes
			io.diskWriteBytes += sw * sectorBytes
		}
		f.Close()
	}

	if f, err := os.Open("/proc/net/dev"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if !strings.Contains(line, ":") {
				continue // header rows
			}
			parts := strings.SplitN(line, ":", 2)
			iface := strings.TrimSpace(parts[0])
			if iface == "lo" {
				continue
			}
			fields := strings.Fields(parts[1])
			if len(fields) < 9 {
				continue
			}
			rx, _ := strconv.ParseUint(fields[0], 10, 64) // rx bytes
			tx, _ := strconv.ParseUint(fields[8], 10, 64) // tx bytes
			io.netRxBytes += rx
			io.netTxBytes += tx
		}
		f.Close()
	}

	return io
}

// perSec converts a counter delta into a rate, guarding against the counter
// resets that happen on reboot (delta would go negative on uint underflow).
func perSec(prev, cur uint64, secs float64) float64 {
	if cur < prev || secs <= 0 {
		return 0
	}
	return float64(cur-prev) / secs
}

// fsUsedPct returns how full a mount point is, via statfs. On Databricks
// classic compute, /local_disk0 is where Spark spills — filling it is a
// common, silent cause of job failure, so it's worth a first-class metric.
func fsUsedPct(path string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return -1 // sentinel: mount not present on this node
	}
	total := st.Blocks * uint64(st.Bsize)
	free := st.Bfree * uint64(st.Bsize)
	if total == 0 {
		return 0
	}
	return float64(total-free) / float64(total) * 100
}
