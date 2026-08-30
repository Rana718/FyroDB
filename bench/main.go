package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
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

func main() {
	port := flag.Int("p", 8000, "server port (single-node mode)")
	mode := flag.String("m", "all", "mode: all | key | pub")
	cluster := flag.String("cluster", "", "comma-separated list of cluster master addrs")
	pid := flag.Int("pid", 0, "server PID for resource monitoring (auto-detect if 0)")
	dockerName := flag.String("docker", "", "docker container name/ID to monitor (auto-detect if empty)")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

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
}
