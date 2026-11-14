// Package sysstat reports this process/container's resource usage — CPU and
// memory from the cgroup (v2, with a v1 fallback), disk from the filesystem,
// and Go runtime stats. It needs no Kubernetes API access, only the read-only
// /sys/fs/cgroup mount and /proc, so it works identically on the VM and in the
// pod.
package sysstat

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var startTime = time.Now()

// Sample is a point-in-time snapshot of resource usage.
type Sample struct {
	// CPU (from the cgroup; limits 0 = unlimited).
	CPUCores      float64 `json:"cpu_cores"`       // cores used over the sample window
	CPULimitCores float64 `json:"cpu_limit_cores"` // 0 = unlimited
	CPUPercent    float64 `json:"cpu_percent"`     // of the limit (0 if unlimited)

	// CPU throttling (cumulative since container start): how often the cgroup
	// was held back at its CPU limit. A non-zero value means the limit is biting.
	CPUThrottledPercent float64 `json:"cpu_throttled_percent"` // throttled periods / total periods
	CPUThrottledSeconds float64 `json:"cpu_throttled_seconds"`

	// Memory (from the cgroup; limit 0 = unlimited).
	MemoryBytes      int64   `json:"memory_bytes"`
	MemoryLimitBytes int64   `json:"memory_limit_bytes"`
	MemoryPercent    float64 `json:"memory_percent"`

	// Disk (the filesystem backing the working directory).
	DiskTotalBytes int64   `json:"disk_total_bytes"`
	DiskUsedBytes  int64   `json:"disk_used_bytes"`
	DiskFreeBytes  int64   `json:"disk_free_bytes"`
	DiskPercent    float64 `json:"disk_percent"`

	// Go runtime.
	Goroutines    int    `json:"goroutines"`
	HeapAllocByte int64  `json:"heap_alloc_bytes"`
	HeapSysBytes  int64  `json:"heap_sys_bytes"`
	GCCycles      uint32 `json:"gc_cycles"`
	NumCPU        int    `json:"num_cpu"`
	GoVersion     string `json:"go_version"`

	// Process.
	UptimeSeconds float64 `json:"uptime_seconds"`
}

// Monitor produces Samples, caching the previous CPU reading so each call can
// report cores used since the last call without blocking. Safe for concurrent
// use.
type Monitor struct {
	mu        sync.Mutex
	lastUsage int64 // cgroup cpu usage in microseconds
	lastAt    time.Time
}

// NewMonitor returns a Monitor with a primed CPU baseline.
func NewMonitor() *Monitor {
	m := &Monitor{}
	if u, ok := cpuUsageMicros(); ok {
		m.lastUsage = u
		m.lastAt = time.Now()
	}
	return m
}

// Sample reads current usage. CPU is the delta since the previous Sample call;
// if there is no usable previous reading it briefly samples over 100ms.
func (m *Monitor) Sample() Sample {
	var s Sample

	// Memory.
	s.MemoryBytes = memoryUsage()
	s.MemoryLimitBytes = memoryLimit()
	if s.MemoryLimitBytes > 0 {
		s.MemoryPercent = pct(s.MemoryBytes, s.MemoryLimitBytes)
	}

	// CPU: delta since last call, else a short window.
	s.CPULimitCores = cpuLimitCores()
	if cores, ok := m.cpuCores(); ok {
		s.CPUCores = cores
		if s.CPULimitCores > 0 {
			s.CPUPercent = s.CPUCores / s.CPULimitCores * 100
		}
	}
	if periods, throttled, throttledUsec, ok := cpuThrottle(); ok && periods > 0 {
		s.CPUThrottledPercent = float64(throttled) / float64(periods) * 100
		s.CPUThrottledSeconds = float64(throttledUsec) / 1e6
	}

	// Disk (platform-specific; returns zeros where unsupported).
	if total, free, ok := diskUsage("/"); ok {
		s.DiskTotalBytes = total
		s.DiskFreeBytes = free
		s.DiskUsedBytes = total - free
		if total > 0 {
			s.DiskPercent = pct(s.DiskUsedBytes, total)
		}
	}

	// Go runtime.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.Goroutines = runtime.NumGoroutine()
	s.HeapAllocByte = int64(ms.HeapAlloc)
	s.HeapSysBytes = int64(ms.HeapSys)
	s.GCCycles = ms.NumGC
	s.NumCPU = runtime.NumCPU()
	s.GoVersion = runtime.Version()

	s.UptimeSeconds = time.Since(startTime).Seconds()
	return s
}

// cpuCores returns cores used since the previous reading. On the first call (or
// if the gap is too small) it samples over a short window so the first paint is
// still meaningful.
func (m *Monitor) cpuCores() (float64, bool) {
	cur, ok := cpuUsageMicros()
	if !ok {
		return 0, false
	}
	now := time.Now()

	m.mu.Lock()
	last, lastAt := m.lastUsage, m.lastAt
	m.mu.Unlock()

	gap := now.Sub(lastAt)
	if lastAt.IsZero() || gap < 50*time.Millisecond {
		// Prime with a short blocking window.
		time.Sleep(100 * time.Millisecond)
		cur2, ok := cpuUsageMicros()
		if !ok {
			return 0, false
		}
		now2 := time.Now()
		cores := float64(cur2-cur) / float64(now2.Sub(now).Microseconds())
		m.mu.Lock()
		m.lastUsage, m.lastAt = cur2, now2
		m.mu.Unlock()
		return cores, true
	}

	cores := float64(cur-last) / float64(gap.Microseconds())
	m.mu.Lock()
	m.lastUsage, m.lastAt = cur, now
	m.mu.Unlock()
	return cores, true
}

func pct(part, whole int64) float64 {
	if whole <= 0 {
		return 0
	}
	return float64(part) / float64(whole) * 100
}

// ---- cgroup readers (v2 preferred, v1 fallback) ----

const cgRoot = "/sys/fs/cgroup"

func readIntFile(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return n, err == nil
}

func memoryUsage() int64 {
	if n, ok := readIntFile(cgRoot + "/memory.current"); ok { // v2
		return n
	}
	if n, ok := readIntFile(cgRoot + "/memory/memory.usage_in_bytes"); ok { // v1
		return n
	}
	return 0
}

// memoryLimit returns the cgroup memory limit in bytes, or 0 if unlimited /
// unknown. Both cgroup versions express "no limit" as a huge sentinel, which we
// treat as unlimited.
func memoryLimit() int64 {
	// v2: memory.max is an integer or the literal "max".
	if b, err := os.ReadFile(cgRoot + "/memory.max"); err == nil {
		s := strings.TrimSpace(string(b))
		if s == "max" {
			return 0
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return saneLimit(n)
		}
	}
	// v1.
	if n, ok := readIntFile(cgRoot + "/memory/memory.limit_in_bytes"); ok {
		return saneLimit(n)
	}
	return 0
}

// saneLimit treats the near-int64-max sentinel (cgroup "no limit") as unlimited.
func saneLimit(n int64) int64 {
	if n <= 0 || n >= (1<<62) {
		return 0
	}
	return n
}

func cpuUsageMicros() (int64, bool) {
	// v2: cpu.stat has "usage_usec N".
	if b, err := os.ReadFile(cgRoot + "/cpu.stat"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if rest, ok := strings.CutPrefix(line, "usage_usec "); ok {
				n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
				return n, err == nil
			}
		}
	}
	// v1: cpuacct.usage is nanoseconds.
	if n, ok := readIntFile(cgRoot + "/cpuacct/cpuacct.usage"); ok {
		return n / 1000, true
	}
	return 0, false
}

// cpuThrottle returns (total periods, throttled periods, throttled microseconds)
// from the cgroup. Supports v2 (cpu.stat) and v1 (cpu/cpu.stat, throttled_time
// in nanoseconds).
func cpuThrottle() (periods, throttled, throttledUsec int64, ok bool) {
	parse := func(b []byte, nsThrottle bool) (int64, int64, int64, bool) {
		var p, t, u int64
		var seen bool
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) != 2 {
				continue
			}
			n, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil {
				continue
			}
			switch f[0] {
			case "nr_periods":
				p, seen = n, true
			case "nr_throttled":
				t = n
			case "throttled_usec":
				u = n
			case "throttled_time": // v1: nanoseconds
				u = n / 1000
			}
		}
		_ = nsThrottle
		return p, t, u, seen
	}
	if b, err := os.ReadFile(cgRoot + "/cpu.stat"); err == nil { // v2
		if p, t, u, seen := parse(b, false); seen {
			return p, t, u, true
		}
	}
	if b, err := os.ReadFile(cgRoot + "/cpu/cpu.stat"); err == nil { // v1
		if p, t, u, seen := parse(b, true); seen {
			return p, t, u, true
		}
	}
	return 0, 0, 0, false
}

// cpuLimitCores returns the CPU limit in cores, or 0 if unlimited.
func cpuLimitCores() float64 {
	// v2: cpu.max = "<quota> <period>"; quota "max" = unlimited.
	if b, err := os.ReadFile(cgRoot + "/cpu.max"); err == nil {
		f := strings.Fields(strings.TrimSpace(string(b)))
		if len(f) == 2 && f[0] != "max" {
			quota, e1 := strconv.ParseFloat(f[0], 64)
			period, e2 := strconv.ParseFloat(f[1], 64)
			if e1 == nil && e2 == nil && period > 0 {
				return quota / period
			}
		}
		return 0
	}
	// v1: cpu.cfs_quota_us / cpu.cfs_period_us; quota -1 = unlimited.
	quota, ok1 := readIntFile(cgRoot + "/cpu/cpu.cfs_quota_us")
	period, ok2 := readIntFile(cgRoot + "/cpu/cpu.cfs_period_us")
	if ok1 && ok2 && quota > 0 && period > 0 {
		return float64(quota) / float64(period)
	}
	return 0
}
