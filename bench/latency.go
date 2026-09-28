package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"
)

// latConfig drives `-m lat`: a latency-first KV phase with explicit value
// size, keyspace and key distribution. With rate > 0 the load is open-loop:
// each request carries its *intended* send time and latency is measured from
// it, so a stalled server shows up as queueing delay instead of silently
// slowing the generator down (coordinated omission).
type latConfig struct {
	rate     int // total target ops/sec; 0 = closed loop
	duration time.Duration
	conns    int
	pipe     int // closed-loop batch size
	vsize    int
	keys     int
	zipf     float64 // 0 = uniform; >1 = zipf exponent
	getRatio float64 // share of GETs in the mix (1 = all GET, 0 = all SET)
}

var latCfg = latConfig{
	duration: 10 * time.Second,
	conns:    50,
	pipe:     1,
	vsize:    64,
	keys:     1_000_000,
	getRatio: 0.9,
}

type keyGen struct {
	r    *rand.Rand
	z    *rand.Zipf
	keys uint64
}

func newKeyGen(seed uint64, cfg latConfig) *keyGen {
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	g := &keyGen{r: r, keys: uint64(cfg.keys)}
	if cfg.zipf > 1 {
		g.z = rand.NewZipf(r, cfg.zipf, 1, g.keys-1)
	}
	return g
}

func (g *keyGen) next() int {
	if g.z != nil {
		return int(g.z.Uint64())
	}
	return int(g.r.Uint64N(g.keys))
}

var latKeyPrefix = []byte("lat:")

func appendLatKey(out []byte, id int) []byte {
	return appendLen(append(out, latKeyPrefix...), id)
}

func appendSetVal(out, key, val []byte) []byte {
	out = appendArrayHdr(out, 3)
	out = appendBulkStr(out, "SET")
	out = appendBulk(out, key)
	return appendBulk(out, val)
}

func appendGet(out, key []byte) []byte {
	out = appendArrayHdr(out, 2)
	out = appendBulkStr(out, "GET")
	return appendBulk(out, key)
}

// preloadKeys writes every key once so GETs hit, using wide pipelines on
// all connections.
func preloadKeys(cfg latConfig, val []byte) {
	conns := preDial(cfg.conns)
	defer closeAll(conns)
	per := (cfg.keys + cfg.conns - 1) / cfg.conns
	var wg sync.WaitGroup
	for ci, c := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(c, 256<<10)
			buf := make([]byte, 0, 512*(len(val)+48))
			var kb [32]byte
			lo, hi := ci*per, min((ci+1)*per, cfg.keys)
			for i := lo; i < hi; {
				n := min(512, hi-i)
				buf = buf[:0]
				for j := range n {
					buf = appendSetVal(buf, appendLatKey(kb[:0], i+j), val)
				}
				writeFull(c, buf)
				discardN(r, n*5)
				i += n
			}
		})
	}
	wg.Wait()
}

func runLatency() {
	cfg := latCfg
	if len(addrs) > 1 {
		fmt.Println("── Latency Benchmark\n   (skipped — single-node only)")
		return
	}
	flushServer()
	val := make([]byte, cfg.vsize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	dist := "uniform"
	if cfg.zipf > 1 {
		dist = fmt.Sprintf("zipf(%.2f)", cfg.zipf)
	}
	mode := "closed-loop"
	if cfg.rate > 0 {
		mode = fmt.Sprintf("open-loop @ %s", fmtRate(float64(cfg.rate)))
	}
	fmt.Printf("── Latency Benchmark (%s) ───────────────────\n", addrs[0])
	fmt.Printf("%s  conns=%d pipe=%d value=%dB keys=%d dist=%s get=%.0f%% dur=%s\n\n",
		mode, cfg.conns, cfg.pipe, cfg.vsize, cfg.keys, dist, cfg.getRatio*100, cfg.duration)

	start := time.Now()
	preloadKeys(cfg, val)
	fmt.Printf("   preload: %d keys in %s\n\n", cfg.keys, time.Since(start).Round(time.Millisecond))

	label := fmt.Sprintf("KV %s v=%dB %s", mode, cfg.vsize, dist)
	var h hist
	var elapsed time.Duration
	if cfg.rate > 0 {
		h, elapsed = openLoop(cfg, val)
	} else {
		h, elapsed = closedLoop(cfg, val)
	}
	recordLatency(label, &h, elapsed)
}

// closedLoop: each connection sends a batch of `pipe` commands and waits
// for all replies; every op in the batch is charged the batch round trip.
func closedLoop(cfg latConfig, val []byte) (hist, time.Duration) {
	conns := preDial(cfg.conns)
	defer closeAll(conns)
	hs := make([]hist, cfg.conns)
	deadline := time.Now().Add(cfg.duration)
	start := time.Now()
	var wg sync.WaitGroup
	for ci, c := range conns {
		wg.Go(func() {
			g := newKeyGen(uint64(ci)+1, cfg)
			r := bufio.NewReaderSize(c, 256<<10)
			buf := make([]byte, 0, cfg.pipe*(len(val)+64))
			var kb [32]byte
			h := &hs[ci]
			for time.Now().Before(deadline) {
				buf = buf[:0]
				for range cfg.pipe {
					key := appendLatKey(kb[:0], g.next())
					if g.r.Float64() < cfg.getRatio {
						buf = appendGet(buf, key)
					} else {
						buf = appendSetVal(buf, key, val)
					}
				}
				t0 := time.Now()
				writeFull(c, buf)
				for range cfg.pipe {
					skipReply(r)
				}
				d := time.Since(t0)
				for range cfg.pipe {
					h.record(d)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	var total hist
	for i := range hs {
		total.merge(&hs[i])
	}
	return total, elapsed
}

// openLoop: a sender per connection emits requests on a fixed schedule and
// queues each request's intended start; a reader pops them in reply order.
func openLoop(cfg latConfig, val []byte) (hist, time.Duration) {
	conns := preDial(cfg.conns)
	defer closeAll(conns)
	perConn := float64(cfg.rate) / float64(cfg.conns)
	interval := time.Duration(float64(time.Second) / perConn)
	hs := make([]hist, cfg.conns)
	start := time.Now()
	end := start.Add(cfg.duration)
	var wg sync.WaitGroup
	for ci, c := range conns {
		pending := make(chan time.Time, 1<<16)
		wg.Go(func() {
			g := newKeyGen(uint64(ci)+1, cfg)
			buf := make([]byte, 0, 64*(len(val)+64))
			var kb [32]byte
			// Stagger connections so their schedules don't align.
			next := start.Add(time.Duration(ci) * interval / time.Duration(cfg.conns))
			for next.Before(end) {
				// Sleep rather than spin: 2×conns spinning goroutines would
				// steal the cores the server under test needs. Timer slack
				// only delays the send, and that delay is charged to the
				// request because latency counts from `next`.
				now := time.Now()
				if now.Before(next) {
					time.Sleep(next.Sub(now))
					continue
				}
				buf = buf[:0]
				// Emit everything that is due (catch up after a stall) in
				// one write, each stamped with its own scheduled time.
				for !next.After(now) && next.Before(end) && len(buf) < 60*(len(val)+64) {
					key := appendLatKey(kb[:0], g.next())
					if g.r.Float64() < cfg.getRatio {
						buf = appendGet(buf, key)
					} else {
						buf = appendSetVal(buf, key, val)
					}
					pending <- next
					next = next.Add(interval)
				}
				writeFull(c, buf)
			}
			close(pending)
		})
		wg.Go(func() {
			r := bufio.NewReaderSize(c, 256<<10)
			h := &hs[ci]
			for intended := range pending {
				skipReply(r)
				h.record(time.Since(intended))
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	var total hist
	for i := range hs {
		total.merge(&hs[i])
	}
	return total, elapsed
}

// writeJSON dumps every throughput and latency result for regression diffs
// (see scripts/bench_compare.py).
func writeJSON(path string) {
	type thr struct {
		Label     string  `json:"label"`
		Ops       int64   `json:"ops"`
		ElapsedNs int64   `json:"elapsed_ns"`
		OpsSec    float64 `json:"ops_per_sec"`
	}
	out := struct {
		Target     string           `json:"target"`
		Time       string           `json:"time"`
		Throughput []thr            `json:"throughput"`
		Latency    []latencySummary `json:"latency"`
		Memory     []memResult      `json:"memory"`
	}{Target: fmt.Sprint(addrs), Time: time.Now().Format(time.RFC3339)}
	for _, r := range mixResults {
		out.Throughput = append(out.Throughput, thr{r.label, r.ops, int64(r.elapsed), r.opsPerSec()})
	}
	out.Latency = latencyResults
	out.Memory = memResults
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "json: %v\n", err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "json: %v\n", err)
		return
	}
	fmt.Printf("results written to %s\n", path)
}

// infoField returns one `field:value` from INFO <section>, or "".
func infoField(addr, section, field string) string {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	out := appendArrayHdr(nil, 2)
	out = appendBulkStr(out, "INFO")
	out = appendBulkStr(out, section)
	conn.Write(out)
	r := bufio.NewReaderSize(conn, 64<<10)
	line, err := r.ReadString('\n')
	if err != nil || len(line) < 2 || line[0] != '$' {
		return ""
	}
	n := 0
	for _, c := range line[1:] {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	body := make([]byte, n+2)
	if _, err := ioReadFull(r, body); err != nil {
		return ""
	}
	prefix := field + ":"
	for _, l := range splitLines(string(body)) {
		if len(l) > len(prefix) && l[:len(prefix)] == prefix {
			return l[len(prefix):]
		}
	}
	return ""
}
