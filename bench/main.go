package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	HOST      = "127.0.0.1"
	PIPE_SIZE = 100
)

var (
	CLIENTS = 100

	OPS_CLIENT = 100_000

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
	mode := flag.String("m", "all", "mode: all | key | pub | mix | vec | lat | mem | ann")
	cluster := flag.String("cluster", "", "comma-separated list of cluster master addrs")
	pid := flag.Int("pid", 0, "server PID for resource monitoring (auto-detect if 0)")
	dockerName := flag.String("docker", "", "docker container name/ID to monitor (auto-detect if empty)")
	noFlush := flag.Bool("f", false, "skip FLUSHALL between phases; keyspace accumulates so peak RSS measures true steady-state memory")
	jsonOut := flag.String("json", "", "write all results (throughput, latency, memory) as JSON to this path")

	flag.IntVar(&PUB_SUBSCRIBERS, "pub-subs", PUB_SUBSCRIBERS, "pub/sub subscribers")
	flag.IntVar(&PUB_PUBLISHERS, "pub-pubs", PUB_PUBLISHERS, "pub/sub publishers")
	flag.IntVar(&PUB_MSGS_EACH, "pub-msgs", PUB_MSGS_EACH, "pub/sub messages per publisher")

	flag.IntVar(&latCfg.rate, "rate", latCfg.rate, "lat: target total ops/sec (0 = closed loop)")
	flag.DurationVar(&latCfg.duration, "dur", latCfg.duration, "lat: measurement duration")
	flag.IntVar(&latCfg.conns, "conns", latCfg.conns, "lat: connections")
	flag.IntVar(&latCfg.pipe, "pipe", latCfg.pipe, "lat: closed-loop pipeline depth")
	flag.IntVar(&latCfg.vsize, "vsize", latCfg.vsize, "lat: value size in bytes")
	flag.IntVar(&latCfg.keys, "keys", latCfg.keys, "lat: keyspace size")
	flag.Float64Var(&latCfg.zipf, "zipf", latCfg.zipf, "lat: zipf exponent (>1 enables, e.g. 1.1)")
	flag.Float64Var(&latCfg.getRatio, "get-ratio", latCfg.getRatio, "lat: share of GETs in the mix")

	flag.StringVar(&annCfg.dir, "ann-dir", "", "ann: directory with *base.fvecs, *query.fvecs, optional *groundtruth.ivecs (empty = synthetic)")
	flag.StringVar(&annCfg.metric, "ann-metric", annCfg.metric, "ann: L2 | COSINE | IP")
	flag.StringVar(&annCfg.algo, "ann-algo", annCfg.algo, "ann: HNSW | FLAT")
	flag.StringVar(&annCfg.quantType, "ann-type", annCfg.quantType, "ann: vector TYPE (FLOAT32 | FLOAT16 | BFLOAT16 | INT8 ...)")
	flag.StringVar(&annCfg.quant, "ann-quant", "", "ann: HNSW QUANTIZATION (NONE | INT8 | RABITQ; empty = server default)")
	flag.IntVar(&annCfg.n, "ann-n", 0, "ann: base vectors to load (0 = all)")
	flag.IntVar(&annCfg.q, "ann-q", annCfg.q, "ann: queries")
	flag.IntVar(&annCfg.k, "ann-k", annCfg.k, "ann: k")
	flag.IntVar(&annCfg.m, "ann-m", annCfg.m, "ann: HNSW M")
	flag.IntVar(&annCfg.efc, "ann-efc", annCfg.efc, "ann: HNSW EF_CONSTRUCTION")
	flag.IntVar(&annCfg.synthN, "ann-synth-n", annCfg.synthN, "ann: synthetic corpus size")
	flag.IntVar(&annCfg.synthDim, "ann-synth-dim", annCfg.synthDim, "ann: synthetic dimensionality")
	efs := flag.String("ann-efs", "10,20,40,80,160,320", "ann: EF_RUNTIME sweep")
	conc := flag.String("ann-conc", "1,8,32", "ann: concurrency sweep")
	filters := flag.String("ann-filters", "1,10,50", "ann: filter selectivity sweep (percent)")
	flag.Parse()
	annCfg.efs = parseIntList(*efs)
	annCfg.conc = parseIntList(*conc)
	annCfg.filters = parseIntList(*filters)

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
		// Single-node: explicit PID, explicit/auto Docker, or native PID.
		if serverPID == 0 {
			if *dockerName != "" {
				dockerContainers = []string{*dockerName}
				useDocker = true
			} else {
				if cid := findDockerContainerOnPort(*port); cid != "" {
					dockerContainers = []string{cid}
					useDocker = true
				} else {
					serverPID = findServerPID(*port)
				}
			}
		}
	}

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

	var mon *monitor
	if useDocker {
		mon = startDockerMonitor(dockerContainers)
	} else if len(clusterPIDs) > 0 {
		mon = startMultiMonitor(clusterPIDs)
	} else {
		mon = startMonitor(serverPID)
	}

	mixResults = nil

	// Warmup: one small pipelined round-trip per connection so the first timed
	// phase skips connection setup, page faults, and cold code paths.
	warmup()

	switch *mode {
	case "key":
		runKV()
	case "pub":
		runPubSub(addrs[0])
	case "mix":
		runMix()
	case "vec":
		runVector()
	case "lat":
		runLatency()
	case "mem":
		runMemory()
	case "ann":
		runANN()
	case "all":
		runKV()
		fmt.Println()
		runPubSub(addrs[0])
		fmt.Println()
		runMix()
		fmt.Println()
		runVector()
	default:
		fmt.Fprintf(os.Stderr, "unknown -m value %q (want: all | key | pub | mix | vec | lat | mem | ann)\n", *mode)
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
		fmt.Println("   CPU scale:   100% = one logical CPU")
	} else {
		fmt.Println("   (no samples collected)")
	}

	if len(mixResults) > 0 {
		printSummaryTable()
	}
	if *jsonOut != "" {
		writeJSON(*jsonOut)
	}

	// With -f the keyspace was never flushed, so report what the server
	// holds now that all phases finished.
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
