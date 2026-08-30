package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	HOST    = "127.0.0.1"
	CLIENTS = 100

	OPS_CLIENT = 10000
	PIPE_SIZE  = 100

	PUB_SUBSCRIBERS = 50
	PUB_PUBLISHERS  = 10
	PUB_MSGS_EACH   = 20000
)

var addrs []string

func pickAddr(i int) string {
	return addrs[i%len(addrs)]
}

// dbsizeOf asks a node for DBSIZE; -1 when unreachable.
func dbsizeOf(addr string) int64 {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return -1
	}
	defer conn.Close()
	conn.Write([]byte("*1\r\n$6\r\nDBSIZE\r\n"))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return -1
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, ":") {
		return -1
	}
	n, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// humanCount prints counts with thousands separators.
func humanCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func main() {
	port := flag.Int("p", 8000, "server port (single-node mode)")
	mode := flag.String("m", "all", "mode: all | key | pub | mix")
	cluster := flag.String("cluster", "", "comma-separated list of cluster master addrs")
	pid := flag.Int("pid", 0, "server PID for resource monitoring (auto-detect if 0)")
	dockerName := flag.String("docker", "", "docker container name/ID to monitor (auto-detect if empty)")
	noFlush := flag.Bool("f", false, "skip FLUSHALL between phases; keyspace accumulates so peak RSS measures true steady-state memory")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Set before any benchmark phase runs; flushServer checks it.
	skipFlush = *noFlush

	if *cluster != "" {
		for _, a := range strings.Split(*cluster, ",") {
			a = strings.TrimSpace(a)
			if a != "" {
				addrs = append(addrs, a)
			}
		}
	} else {
		addrs = []string{fmt.Sprintf("%s:%d", HOST, *port)}
	}

	for _, addr := range addrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not connect to %s: %v\n", addr, err)
			os.Exit(1)
		}
		conn.Close()
	}

	if len(addrs) == 1 {
		fmt.Printf("connected to %s\n\n", addrs[0])
	} else {
		fmt.Printf("connected to Redis Cluster (%d masters): %s\n\n",
			len(addrs), strings.Join(addrs, ", "))
	}

	// ── Resource monitoring setup ─────────────────────────────────────────────
	serverPID := *pid
	var clusterPIDs []int
	var dockerContainers []string
	useDocker := false

	if len(addrs) > 1 {
		// Cluster mode: prefer Docker containers, fall back to native PIDs.
		if *dockerName != "" {
			dockerContainers = []string{*dockerName}
			useDocker = true
		} else {
			dockerContainers = findRedisContainers()
			if len(dockerContainers) > 0 {
				useDocker = true
			} else {
				for _, addr := range addrs {
					parts := strings.Split(addr, ":")
					if len(parts) == 2 {
						p, _ := strconv.Atoi(parts[1])
						if rpid := findServerPID(p); rpid > 0 {
							clusterPIDs = append(clusterPIDs, rpid)
						}
					}
				}
			}
		}
	} else {
		// Single-node mode: explicit PID, explicit Docker name, auto-detect Docker, or native PID.
		if serverPID == 0 {
			if *dockerName != "" {
				dockerContainers = []string{*dockerName}
				useDocker = true
			} else {
				// Try to find a Docker container mapped to this port.
				if cid := findDockerContainerOnPort(*port); cid != "" {
					dockerContainers = []string{cid}
					useDocker = true
				} else {
					serverPID = findServerPID(*port)
				}
			}
		}
	}

	// ── Print idle resource stats ─────────────────────────────────────────────
	if serverPID > 0 {
		idle := sampleProc(serverPID)
		fmt.Printf("── Server Resource (idle, PID %d) ──────────────\n", serverPID)
		fmt.Printf("   RSS: %s\n\n", fmtBytes(idle.rssBytes))
	} else if useDocker {
		rss, cpu := sampleDocker(dockerContainers)
		label := "container"
		if len(dockerContainers) > 1 {
			label = fmt.Sprintf("%d containers", len(dockerContainers))
		}
		fmt.Printf("── Server Resource (idle, %s) ──────\n", label)
		fmt.Printf("   total RSS: %s  CPU: %.1f%%\n\n", fmtBytes(rss), cpu)
	} else if len(clusterPIDs) > 0 {
		var totalRSS int64
		for _, p := range clusterPIDs {
			s := sampleProc(p)
			totalRSS += s.rssBytes
		}
		fmt.Printf("── Cluster Resource (idle, %d nodes) ───────────\n", len(clusterPIDs))
		fmt.Printf("   total RSS: %s\n\n", fmtBytes(totalRSS))
	} else {
		fmt.Printf("── Server Resource (idle) ─── (no process/container found)\n\n")
	}

	// ── Start monitor ─────────────────────────────────────────────────────────
	var mon *monitor
	if useDocker {
		mon = startDockerMonitor(dockerContainers)
	} else if len(clusterPIDs) > 0 {
		mon = startMultiMonitor(clusterPIDs)
	} else {
		mon = startMonitor(serverPID)
	}

	mixResults = nil

	// Warmup: one small pipelined round-trip per connection so the first
	// timed phase does not pay connection setup, page faults and cold code
	// paths. Too small to affect the server's own caches meaningfully.
	warmup()

	switch *mode {
	case "key":
		runKV()
	case "pub":
		runPubSub(addrs[0])
	case "mix":
		runMix()
	case "all":
		runKV()
		fmt.Println()
		runPubSub(addrs[0])
		fmt.Println()
		runMix()
	default:
		fmt.Fprintf(os.Stderr, "unknown -m value %q (want: all | key | pub | mix)\n", *mode)
		os.Exit(1)
	}

	mon.stop()

	fmt.Println()
	fmt.Println("── Server Resource Usage ────────────────────────")
	if serverPID > 0 {
		fmt.Printf("   PID:         %d\n", serverPID)
	} else if useDocker {
		if len(dockerContainers) == 1 {
			fmt.Printf("   container:   %s\n", dockerContainers[0])
		} else {
			fmt.Printf("   containers:  %d\n", len(dockerContainers))
		}
	} else if len(clusterPIDs) > 0 {
		fmt.Printf("   nodes:       %d\n", len(clusterPIDs))
	} else {
		fmt.Println("   (no process/container found — resource stats unavailable)")
	}
	if mon.peakRSS > 0 || mon.peakCPU > 0 {
		fmt.Printf("   peak RSS:    %s\n", fmtBytes(mon.peakRSS))
		fmt.Printf("   avg RSS:     %s\n", fmtBytes(mon.avgRSS))
		fmt.Printf("   peak CPU:    %.1f%%\n", mon.peakCPU)
		fmt.Printf("   avg CPU:     %.1f%%\n", mon.avgCPU)
	} else {
		fmt.Println("   (no samples collected)")
	}

	if len(mixResults) > 0 {
		printSummaryTable()
	}

	// With -f the keyspace was never flushed, so report what the server is
	// actually holding now that all phases finished.
	if skipFlush {
		fmt.Println("── Final State (-f: no flush between phases) ────")
		for _, addr := range addrs {
			n := dbsizeOf(addr)
			if n >= 0 {
				fmt.Printf("   %s: %s keys\n", addr, humanCount(n))
			}
		}
		if serverPID > 0 {
			s := sampleProc(serverPID)
			fmt.Printf("   final RSS (PID %d): %s\n", serverPID, fmtBytes(s.rssBytes))
		} else if useDocker {
			rss, _ := sampleDocker(dockerContainers)
			fmt.Printf("   final RSS (docker): %s\n", fmtBytes(rss))
		} else if len(clusterPIDs) > 0 {
			var total int64
			for _, p := range clusterPIDs {
				total += sampleProc(p).rssBytes
			}
			fmt.Printf("   final RSS (%d nodes): %s\n", len(clusterPIDs), fmtBytes(total))
		}
		fmt.Println()
	}
}
