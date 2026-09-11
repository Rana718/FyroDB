package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type procSample struct {
	rssBytes  int64
	cpuTotal  int64
	timestamp time.Time
}

type monitor struct {
	pid     int
	pids    []int
	done    chan struct{}
	wg      sync.WaitGroup
	peakRSS int64
	avgRSS  int64
	peakCPU float64
	avgCPU  float64
}

func startMonitor(pid int) *monitor {
	m := &monitor{pid: pid, done: make(chan struct{})}
	if pid <= 0 {
		return m
	}
	m.wg.Add(1)
	go m.run()
	return m
}

func startMultiMonitor(pids []int) *monitor {
	m := &monitor{pid: 0, pids: pids, done: make(chan struct{})}
	if len(pids) == 0 {
		return m
	}
	m.wg.Add(1)
	go m.runMulti()
	return m
}

func (m *monitor) stop() {
	close(m.done)
	m.wg.Wait()
}

func (m *monitor) runMulti() {
	defer m.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	numCPU := float64(runtime.NumCPU())
	var rssSamples []int64
	var cpuSamples []float64

	prevCPU := make([]int64, len(m.pids))
	for i, pid := range m.pids {
		s := sampleProc(pid)
		prevCPU[i] = s.cpuTotal
	}
	prevTime := time.Now()

	for {
		select {
		case <-m.done:
			goto finish
		case <-ticker.C:
			var totalRSS int64
			var totalCPUDelta int64
			now := time.Now()

			for i, pid := range m.pids {
				s := sampleProc(pid)
				totalRSS += s.rssBytes
				totalCPUDelta += s.cpuTotal - prevCPU[i]
				prevCPU[i] = s.cpuTotal
			}

			rssSamples = append(rssSamples, totalRSS)
			if totalRSS > m.peakRSS {
				m.peakRSS = totalRSS
			}

			dt := now.Sub(prevTime).Seconds()
			if dt > 0 {
				ticksPerSec := float64(getClkTck())
				cpuPct := (float64(totalCPUDelta) / ticksPerSec / dt) * 100.0 / numCPU
				cpuSamples = append(cpuSamples, cpuPct)
				if cpuPct > m.peakCPU {
					m.peakCPU = cpuPct
				}
			}
			prevTime = now
		}
	}

finish:
	if len(rssSamples) > 0 {
		var total int64
		for _, v := range rssSamples {
			total += v
		}
		m.avgRSS = total / int64(len(rssSamples))
	}
	if len(cpuSamples) > 0 {
		var total float64
		for _, v := range cpuSamples {
			total += v
		}
		m.avgCPU = total / float64(len(cpuSamples))
	}
}

func (m *monitor) run() {
	defer m.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var samples []int64
	var prevCPU int64
	var prevTime time.Time
	var cpuSamples []float64
	numCPU := float64(runtime.NumCPU())

	first := sampleProc(m.pid)
	prevCPU = first.cpuTotal
	prevTime = first.timestamp

	for {
		select {
		case <-m.done:
			goto finish
		case <-ticker.C:
			s := sampleProc(m.pid)
			if s.rssBytes <= 0 {
				continue
			}

			samples = append(samples, s.rssBytes)
			if s.rssBytes > m.peakRSS {
				m.peakRSS = s.rssBytes
			}

			dt := s.timestamp.Sub(prevTime).Seconds()
			if dt > 0 {
				ticksPerSec := float64(getClkTck())
				cpuDelta := float64(s.cpuTotal-prevCPU) / ticksPerSec
				cpuPct := (cpuDelta / dt) * 100.0 / numCPU
				cpuSamples = append(cpuSamples, cpuPct)
				if cpuPct > m.peakCPU {
					m.peakCPU = cpuPct
				}
			}
			prevCPU = s.cpuTotal
			prevTime = s.timestamp
		}
	}

finish:
	if len(samples) > 0 {
		var total int64
		for _, v := range samples {
			total += v
		}
		m.avgRSS = total / int64(len(samples))
	}
	if len(cpuSamples) > 0 {
		var total float64
		for _, v := range cpuSamples {
			total += v
		}
		m.avgCPU = total / float64(len(cpuSamples))
	}
}

func sampleProc(pid int) procSample {
	if pid <= 0 {
		return procSample{}
	}
	now := time.Now()

	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	data, err := os.ReadFile(statPath)
	if err != nil {
		return procSample{timestamp: now}
	}

	s := string(data)
	closeP := strings.LastIndex(s, ")")
	if closeP < 0 {
		return procSample{timestamp: now}
	}
	fields := strings.Fields(s[closeP+2:])

	var cpuTotal int64
	if len(fields) > 12 {
		utime, _ := strconv.ParseInt(fields[11], 10, 64)
		stime, _ := strconv.ParseInt(fields[12], 10, 64)
		cpuTotal = utime + stime
	}

	statmPath := fmt.Sprintf("/proc/%d/statm", pid)
	data2, err := os.ReadFile(statmPath)
	if err != nil {
		return procSample{cpuTotal: cpuTotal, timestamp: now}
	}
	fields2 := strings.Fields(string(data2))
	var rss int64
	if len(fields2) > 1 {
		pages, _ := strconv.ParseInt(fields2[1], 10, 64)
		rss = pages * 4096
	}

	return procSample{
		rssBytes:  rss,
		cpuTotal:  cpuTotal,
		timestamp: now,
	}
}

func findServerPID(port int) int {
	// 1. Try ss (more reliable than lsof, available without extra packages)
	out, err := exec.Command("sh", "-c",
		fmt.Sprintf("ss -tlnp sport = :%d 2>/dev/null | awk 'NR>1{print $NF}'", port)).Output()
	if err == nil {
		// ss output format: users:(("fyro_db",pid=12345,fd=7))
		s := string(out)
		if idx := strings.Index(s, "pid="); idx >= 0 {
			rest := s[idx+4:]
			end := strings.IndexAny(rest, ",)")
			if end > 0 {
				if pid, err := strconv.Atoi(rest[:end]); err == nil {
					return pid
				}
			}
		}
	}

	// 2. Try lsof as fallback
	out, err = exec.Command("sh", "-c",
		fmt.Sprintf("lsof -ti tcp:%d -s tcp:listen 2>/dev/null | head -1", port)).Output()
	if err == nil {
		s := strings.TrimSpace(string(out))
		if pid, err := strconv.Atoi(s); err == nil {
			return pid
		}
	}

	// 3. Scan /proc/*/comm for known binary names
	for _, name := range []string{"fyro_db", "redis-server", "dragonfly"} {
		matches, _ := filepath.Glob("/proc/*/comm")
		for _, m := range matches {
			data, err := os.ReadFile(m)
			if err == nil && strings.TrimSpace(string(data)) == name {
				parts := strings.Split(m, "/")
				if len(parts) >= 3 {
					if pid, err := strconv.Atoi(parts[2]); err == nil {
						return pid
					}
				}
			}
		}
	}

	return 0
}

func findDockerContainerOnPort(port int) string {
	out, err := exec.Command("docker", "ps",
		"--format", "{{.ID}} {{.Image}} {{.Ports}}").Output()
	if err != nil {
		return ""
	}

	portStr := fmt.Sprintf("%d->", port)
	var hostNetCandidates []string

	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		if strings.Contains(line, portStr) {
			return parts[0]
		}

		if len(parts) == 2 || (len(parts) >= 3 && strings.TrimSpace(strings.Join(parts[2:], "")) == "") {
			hostNetCandidates = append(hostNetCandidates, parts[0])
		}
	}

	for _, cid := range hostNetCandidates {
		netOut, err := exec.Command("docker", "inspect", "--format",
			"{{.HostConfig.NetworkMode}}", cid).Output()
		if err == nil && strings.TrimSpace(string(netOut)) == "host" {
			return cid
		}
	}

	return ""
}

var clkTck int64

func getClkTck() int64 {
	if clkTck > 0 {
		return clkTck
	}
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil {
			clkTck = v
			return v
		}
	}
	clkTck = 100
	return 100
}

func fmtBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func findRedisContainers() []string {

	out, err := exec.Command("docker", "ps", "--format", "{{.ID}} {{.Image}}").Output()
	if err != nil {
		return nil
	}
	var ids []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.Fields(line)
		if len(parts) >= 2 && strings.Contains(strings.ToLower(parts[1]), "redis") {
			ids = append(ids, parts[0])
		}
	}
	return ids
}

func sampleDocker(containers []string) (rssBytes int64, cpuPct float64) {
	if len(containers) == 0 {
		return 0, 0
	}
	args := []string{"stats", "--no-stream", "--format", "{{.MemUsage}} {{.CPUPerc}}"}
	args = append(args, containers...)
	out, err := exec.Command("docker", args...).Output()
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		mem, cpu := parseDockerStatsLine(line)
		rssBytes += mem
		cpuPct += cpu
	}
	return
}

func parseDockerStatsLine(line string) (memBytes int64, cpuPct float64) {

	parts := strings.Fields(line)
	if len(parts) >= 1 {
		memBytes = parseDockerMem(parts[0])
	}

	for _, p := range parts {
		if strings.HasSuffix(p, "%") {
			p = strings.TrimSuffix(p, "%")
			v, _ := strconv.ParseFloat(p, 64)
			cpuPct = v
		}
	}
	return
}

func parseDockerMem(s string) int64 {
	s = strings.TrimSpace(s)
	multiplier := int64(1)
	if strings.HasSuffix(s, "GiB") {
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "GiB")
	} else if strings.HasSuffix(s, "MiB") {
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "MiB")
	} else if strings.HasSuffix(s, "KiB") {
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "KiB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1_000_000_000
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1_000_000
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "kB") {
		multiplier = 1_000
		s = strings.TrimSuffix(s, "KB")
		s = strings.TrimSuffix(s, "kB")
	}
	v, _ := strconv.ParseFloat(s, 64)
	return int64(v * float64(multiplier))
}

func startDockerMonitor(containers []string) *monitor {
	m := &monitor{done: make(chan struct{})}
	if len(containers) == 0 {
		return m
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		var rssSamples []int64
		var cpuSamples []float64

		for {
			select {
			case <-m.done:
				goto finish
			case <-ticker.C:
				rss, cpu := sampleDocker(containers)
				if rss > 0 {
					rssSamples = append(rssSamples, rss)
					if rss > m.peakRSS {
						m.peakRSS = rss
					}
				}
				if cpu > 0 {
					cpuSamples = append(cpuSamples, cpu)
					if cpu > m.peakCPU {
						m.peakCPU = cpu
					}
				}
			}
		}

	finish:
		if len(rssSamples) > 0 {
			var total int64
			for _, v := range rssSamples {
				total += v
			}
			m.avgRSS = total / int64(len(rssSamples))
		}
		if len(cpuSamples) > 0 {
			var total float64
			for _, v := range cpuSamples {
				total += v
			}
			m.avgCPU = total / float64(len(cpuSamples))
		}
	}()
	return m
}
