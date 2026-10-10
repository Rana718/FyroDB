package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MIX_PIPE = 100

var (
	MIX_OPS_CLIENT = 10000
	MIX_HOTKEY_OPS = 50000
	MIX_QUEUE_OPS  = 10000
)

var (
	// skipFlush (-f): keep all keys between phases so final peak RSS
	// reflects the full workload's steady-state footprint.
	skipFlush bool

	incrHdr     = []byte("*2\r\n$4\r\nINCR\r\n$")
	hsetHdr     = []byte("*4\r\n$4\r\nHSET\r\n$")
	hgetHdr     = []byte("*3\r\n$4\r\nHGET\r\n$")
	lpushHdr    = []byte("*3\r\n$5\r\nLPUSH\r\n$")
	rpopHdr     = []byte("*2\r\n$4\r\nRPOP\r\n$")
	saddHdr     = []byte("*3\r\n$4\r\nSADD\r\n$")
	zaddHdr     = []byte("*4\r\n$4\r\nZADD\r\n$")
	expireHdr   = []byte("*3\r\n$6\r\nEXPIRE\r\n$")
	fieldSuffix = []byte("\r\n$1\r\nf\r\n$1\r\nv\r\n")
	fieldGet    = []byte("\r\n$1\r\nf\r\n")
	lpushBody   = []byte("\r\n$7\r\npayload\r\n")
	expireTTL   = []byte("\r\n$2\r\n60\r\n")
	flushCmd    = []byte("*1\r\n$8\r\nFLUSHALL\r\n")
)

type benchResult struct {
	label   string
	ops     int64
	elapsed time.Duration
}

func (b benchResult) opsPerSec() float64 {
	return float64(b.ops) / b.elapsed.Seconds()
}

var mixResults []benchResult

func clientKey(prefix string, id int, scratch []byte) []byte {
	key := scratch[:0]
	if len(addrs) > 1 {
		key = append(key, clusterHashTags()[id%len(addrs)]...)
	}
	key = append(key, prefix...)
	return strconv.AppendInt(key, int64(id), 10)
}

func sharedClusterKey(name string) []byte {
	if len(addrs) == 1 {
		return []byte(name)
	}
	key := append([]byte(nil), clusterHashTags()[0]...)
	return append(key, name...)
}

func recordResult(label string, ops int64, elapsed time.Duration) {
	printResult(label, ops, elapsed)
	mixResults = append(mixResults, benchResult{label, ops, elapsed})
}

func flushServer() {
	if skipFlush {
		return
	}
	for _, addr := range addrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		conn.Write(flushCmd)
		conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		r := bufio.NewReaderSize(conn, 256)
		r.ReadSlice('\n')
		r.ReadSlice('\n')
		conn.Close()
	}
}

func runMix() {
	if len(addrs) > 1 {
		runMixCluster()
		return
	}

	fmt.Printf("── Mixed Workload Benchmark (%s) ───────────────\n", addrs[0])
	fmt.Printf("clients=%d  ops/client=%d  pipeline=%d\n\n", CLIENTS, MIX_OPS_CLIENT, MIX_PIPE)

	phases := []struct {
		name string
		run  func()
	}{
		{"mixed", runBenchMixed}, {"incr", runBenchIncr}, {"hash", runBenchHash}, {"list", runBenchList},
		{"set", runBenchSet}, {"zset", runBenchZSet}, {"json", runBenchJson}, {"expire", runBenchExpire},
		{"hotkey", runBenchHotKey}, {"queue", runBenchQueue}, {"stream", runBenchStream},
		{"bitmap", runBenchBitmap}, {"hll", runBenchHLL}, {"geo", runBenchGeo},
	}
	only := os.Getenv("MIX_ONLY")
	repeat := 1
	if n, err := strconv.Atoi(os.Getenv("MIX_REPEAT")); err == nil && n > 1 {
		repeat = n
	}
	for _, p := range phases {
		if only != "" && !slices.Contains(strings.Split(only, ","), p.name) {
			continue
		}
		for range repeat {
			p.run()
			flushServer()
		}
	}
}

func runMixCluster() {
	fmt.Printf("── Mixed Workload Benchmark (cluster %d masters) ────\n", len(addrs))
	fmt.Printf("clients=%d  ops/client=%d  pipeline=%d\n\n", CLIENTS, MIX_OPS_CLIENT, MIX_PIPE)

	runBenchMixed()
	flushServer()
	runBenchIncr()
	flushServer()
	runBenchHash()
	flushServer()
	runBenchList()
	flushServer()
	runBenchSet()
	flushServer()
	runBenchZSet()
	flushServer()
	runBenchJson()
	flushServer()
	runBenchExpire()
	flushServer()
	runBenchHotKey()
	flushServer()
	runBenchQueue()
	flushServer()
	runBenchStream()
	flushServer()
	runBenchBitmap()
	flushServer()
	runBenchHLL()
	flushServer()
	runBenchGeo()
	flushServer()
}

func printSummaryTable() {
	fmt.Println()
	fmt.Println("── Summary ─────────────────────────────────────")
	fmt.Printf("  %-32s %10s %14s\n", "Benchmark", "Elapsed", "Throughput")
	fmt.Printf("  %-32s %10s %14s\n", "────────────────────────────────", "──────────", "──────────────")
	for _, r := range mixResults {
		fmt.Printf("  %-32s %10s %14s\n", r.label, r.elapsed.Round(time.Millisecond), fmtRate(r.opsPerSec()))
	}
	fmt.Println()
}

func runBenchMixed() {
	if len(addrs) > 1 {
		runClusterMixed()
		return
	}
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 256<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb [32]byte
			base := i * MIX_OPS_CLIENT

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for j := range sets {
					kn := appendLen(append(kb[:0], "mix:"...), base+(sent/2)+j)
					buf = appendSetBytes(buf, kn)
				}
				for j := range gets {
					kn := appendLen(append(kb[:0], "mix:"...), base+(sent/2)+j)
					buf = appendGetBytes(buf, kn)
				}
				writeFull(conn, buf)
				discardN(r, sets*5)
				skipGetReplies(r, gets)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("Mixed SET/GET (50/50)", total, time.Since(start))
}

func runClusterMixed() {
	conns := preDialCluster(CLIENTS)
	defer closeCluster(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)
	start := time.Now()
	var wg sync.WaitGroup
	for id, nodeConns := range conns {
		wg.Go(func() {
			conns := nodeConns
			readers := makeReaders(conns, 128<<10)
			bufs := makeNodeBufs(len(conns), MIX_PIPE*64)
			cnts := make([]int, len(conns))
			tags := clusterHashTags()
			var kb [64]byte
			base := id * MIX_OPS_CLIENT
			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				resetBufs(bufs, cnts)
				sets, gets := make([]int, len(conns)), make([]int, len(conns))
				half := (batch + 1) / 2
				for j := range half {
					key := appendLen(append(kb[:0], tags[id%len(tags)]...), base+sent/2+j)
					n := nodeForKey(key)
					bufs[n] = appendSetBytes(bufs[n], key)
					cnts[n]++
					sets[n]++
				}
				for j := range batch / 2 {
					key := appendLen(append(kb[:0], tags[id%len(tags)]...), base+sent/2+j)
					n := nodeForKey(key)
					bufs[n] = appendGetBytes(bufs[n], key)
					cnts[n]++
					gets[n]++
				}
				flushAll(conns, bufs, cnts)
				for n := range conns {
					discardN(readers[n], sets[n]*5)
					skipGetReplies(readers[n], gets[n])
				}
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("Mixed SET/GET (50/50)", total, time.Since(start))
}

func runBenchIncr() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*28)
			var kb [32]byte
			key := clientKey("incr:", i%100, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for range batch {
					buf = append(buf, incrHdr...)
					buf = appendLen(buf, len(key))
					buf = append(buf, crlfB...)
					buf = append(buf, key...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("INCR (atomic counters)", total, time.Since(start))
}

func runBenchHash() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb [32]byte
			key := clientKey("sess:", i, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				hsets := (batch + 1) / 2
				hgets := batch / 2
				for phase := 0; phase < 2; phase++ {
					count := hsets
					if phase == 1 {
						count = hgets
					}
					for j := 0; j < count; j++ {
						if phase == 0 {
							buf = append(buf, hsetHdr...)
							buf = appendLen(buf, len(key))
							buf = append(buf, crlfB...)
							buf = append(buf, key...)
							buf = append(buf, fieldSuffix...)
						} else {
							buf = append(buf, hgetHdr...)
							buf = appendLen(buf, len(key))
							buf = append(buf, crlfB...)
							buf = append(buf, key...)
							buf = append(buf, fieldGet...)
						}
					}
				}
				writeFull(conn, buf)
				skipLines(r, hsets)
				skipGetReplies(r, hgets)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("HSET/HGET (sessions)", total, time.Since(start))
}

func runBenchList() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb [32]byte
			qkey := clientKey("q:", i, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				pushes := (batch + 1) / 2
				pops := batch / 2
				for phase := 0; phase < 2; phase++ {
					count := pushes
					if phase == 1 {
						count = pops
					}
					for j := 0; j < count; j++ {
						if phase == 0 {
							buf = append(buf, lpushHdr...)
							buf = appendLen(buf, len(qkey))
							buf = append(buf, crlfB...)
							buf = append(buf, qkey...)
							buf = append(buf, lpushBody...)
						} else {
							buf = append(buf, rpopHdr...)
							buf = appendLen(buf, len(qkey))
							buf = append(buf, crlfB...)
							buf = append(buf, qkey...)
							buf = append(buf, crlfB...)
						}
					}
				}
				writeFull(conn, buf)
				skipLines(r, pushes)
				skipGetReplies(r, pops)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("LPUSH/RPOP (queue)", total, time.Since(start))
}

func runBenchSet() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb, mb [32]byte
			setkey := clientKey("s:", i, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					member := appendLen(mb[:0], sent+j)
					buf = append(buf, saddHdr...)
					buf = appendLen(buf, len(setkey))
					buf = append(buf, crlfB...)
					buf = append(buf, setkey...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(member))
					buf = append(buf, crlfB...)
					buf = append(buf, member...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("SADD (per-client sets)", total, time.Since(start))
}

func runBenchZSet() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb, mb [32]byte
			lbkey := clientKey("lb:", i, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					member := appendLen(mb[:0], sent+j)
					buf = append(buf, zaddHdr...)
					buf = appendLen(buf, len(lbkey))
					buf = append(buf, crlfB...)
					buf = append(buf, lbkey...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(member))
					buf = append(buf, crlfB...)
					buf = append(buf, member...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(member))
					buf = append(buf, crlfB...)
					buf = append(buf, member...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("ZADD (per-client zsets)", total, time.Since(start))
}

func runBenchExpire() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*80)
			var kb [32]byte
			base := i * MIX_OPS_CLIENT
			prefix := []byte(nil)
			if len(addrs) > 1 {
				prefix = clusterHashTags()[i%len(addrs)]
			}

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					kn := appendLen(append(kb[:0], prefix...), base+sent+j)
					buf = appendSetBytes(buf, kn)
					buf = append(buf, expireHdr...)
					buf = appendLen(buf, len(kn))
					buf = append(buf, crlfB...)
					buf = append(buf, kn...)
					buf = append(buf, expireTTL...)
				}
				writeFull(conn, buf)
				skipLines(r, batch*2)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("SET+EXPIRE (cache TTL)", total, time.Since(start))
}

func runBenchHotKey() {
	conns := preDialTo(CLIENTS, 0)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_HOTKEY_OPS)

	start := time.Now()
	var wg sync.WaitGroup
	hotkey := sharedClusterKey("hot")
	for _, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*32)

			for sent := 0; sent < MIX_HOTKEY_OPS; {
				batch := min(MIX_PIPE, MIX_HOTKEY_OPS-sent)
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for range sets {
					buf = appendSetBytes(buf, hotkey)
				}
				for range gets {
					buf = appendGetBytes(buf, hotkey)
				}
				writeFull(conn, buf)
				discardN(r, sets*5)
				skipGetReplies(r, gets)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("Hot Key (1 key, contention)", total, time.Since(start))
}

func runBenchQueue() {
	half := CLIENTS / 2
	if half < 1 {
		half = 1
	}
	producers := preDialTo(half, 0)
	consumers := preDialTo(half, 0)
	defer closeAll(producers)
	defer closeAll(consumers)
	total := int64(half * MIX_QUEUE_OPS * 2)
	qkey := sharedClusterKey("wq")

	start := time.Now()
	var wg sync.WaitGroup

	for i := range half {
		wg.Go(func() {
			conn := producers[i]
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*40)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := min(MIX_PIPE, MIX_QUEUE_OPS-sent)
				buf = buf[:0]
				for range batch {
					buf = append(buf, lpushHdr...)
					buf = appendLen(buf, len(qkey))
					buf = append(buf, crlfB...)
					buf = append(buf, qkey...)
					buf = append(buf, lpushBody...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}
		})
	}

	for i := range half {
		wg.Go(func() {
			conn := consumers[i]
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*28)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := min(MIX_PIPE, MIX_QUEUE_OPS-sent)
				buf = buf[:0]
				for range batch {
					buf = append(buf, rpopHdr...)
					buf = appendLen(buf, len(qkey))
					buf = append(buf, crlfB...)
					buf = append(buf, qkey...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipGetReplies(r, batch)
				sent += batch
			}
		})
	}

	wg.Wait()
	recordResult("Producer/Consumer (50+50)", total, time.Since(start))
}

func runBenchJson() {
	supported := false
	for _, addr := range addrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		conn.Write([]byte("*4\r\n$8\r\nJSON.SET\r\n$7\r\n__probe\r\n$1\r\n.\r\n$4\r\nnull\r\n"))
		probe := make([]byte, 256)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(probe)
		conn.Close()
		if n > 0 && (probe[0] == '+' || probe[0] == ':') {
			supported = true
			break
		}
		if n > 0 && probe[0] == '-' {
			reply := string(probe[:n])
			if strings.Contains(reply, "MOVED") || strings.Contains(reply, "ASK") {
				supported = true
				break
			}
		}
	}
	if !supported {
		fmt.Println("── JSON.SET/GET (documents)\n   (skipped — JSON module not loaded)")
		return
	}

	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	jsonSetHdr := []byte("*4\r\n$8\r\nJSON.SET\r\n$")
	jsonGetHdr := []byte("*3\r\n$8\r\nJSON.GET\r\n$")
	pathDot := []byte("\r\n$1\r\n.\r\n")
	pathDotVal := []byte("\r\n$1\r\n.\r\n$27\r\n{\"name\":\"test\",\"score\":100}\r\n")

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*96)
			var kb [32]byte
			base := i * MIX_OPS_CLIENT
			prefix := []byte("json:")
			if len(addrs) > 1 {
				prefix = append(append([]byte(nil), clusterHashTags()[i%len(addrs)]...), prefix...)
			}

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for j := range sets {
					kn := appendLen(append(kb[:0], prefix...), base+(sent/2)+j)
					buf = append(buf, jsonSetHdr...)
					buf = appendLen(buf, len(kn))
					buf = append(buf, crlfB...)
					buf = append(buf, kn...)
					buf = append(buf, pathDotVal...)
				}
				for j := range gets {
					kn := appendLen(append(kb[:0], prefix...), base+(sent/2)+j)
					buf = append(buf, jsonGetHdr...)
					buf = appendLen(buf, len(kn))
					buf = append(buf, crlfB...)
					buf = append(buf, kn...)
					buf = append(buf, pathDot...)
				}
				writeFull(conn, buf)
				skipLines(r, sets)
				skipGetReplies(r, gets)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("JSON.SET/GET (documents)", total, time.Since(start))
}

// Each client pipelined-appends entries to its own stream key. XADD returns a
// variable-length entry-ID bulk string, so use skipBulkReply, not skipGetReplies.

var xaddHdr = []byte("*5\r\n$4\r\nXADD\r\n$")

func appendXADD(out, key []byte, seq int) []byte {
	// XADD <key> * f <seq>
	out = append(out, xaddHdr...)
	out = appendLen(out, len(key))
	out = append(out, crlfB...)
	out = append(out, key...)
	out = append(out, "\r\n$1\r\n*\r\n$1\r\nf\r\n$"...)
	sv := strconv.AppendInt(nil, int64(seq), 10)
	out = appendLen(out, len(sv))
	out = append(out, crlfB...)
	out = append(out, sv...)
	out = append(out, crlfB...)
	return out
}

// skipBulkReply drains n RESP bulk-string replies ($<len>\r\n<data>\r\n).
func skipBulkReply(r *bufio.Reader, n int) {
	for i := 0; i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			panic(err)
		}
		switch b {
		case '$':
			line, err := r.ReadSlice('\n')
			if err != nil {
				panic(err)
			}
			vlen := 0
			for _, c := range line {
				if c >= '0' && c <= '9' {
					vlen = vlen*10 + int(c-'0')
				} else {
					break
				}
			}
			if vlen < 0 {
				continue // null bulk string ($-1\r\n)
			}
			discardN(r, vlen+2)
		case '+', ':', '-':
			// simple string, integer, or error: read to end of line
			for {
				line, e := r.ReadSlice('\n')
				if e != bufio.ErrBufferFull {
					if b == '-' {
						panic("Redis error: " + string(line))
					}
					break
				}
			}
		default:
			panic("unexpected RESP type byte: " + string([]byte{b}))
		}
	}
}

func runBenchStream() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 256<<10)
			buf := make([]byte, 0, MIX_PIPE*80)
			var kb [32]byte
			skey := clientKey("stream:", i, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					buf = appendXADD(buf, skey, sent+j)
				}
				writeFull(conn, buf)
				skipBulkReply(r, batch)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("XADD (streams)", total, time.Since(start))
}

// Bitmap: each client sets MIX_OPS_CLIENT bits on its own key, then one
// BITCOUNT. Ops counted = SETBIT calls.

var (
	setbitHdr   = []byte("*4\r\n$6\r\nSETBIT\r\n$")
	bitcountHdr = []byte("*2\r\n$8\r\nBITCOUNT\r\n$")
)

func runBenchBitmap() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 256<<10)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb [32]byte
			bkey := clientKey("bmap:", i, kb[:])

			// Phase 1: SETBIT <key> <offset> 1
			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					offset := strconv.AppendInt(nil, int64(sent+j), 10)
					buf = append(buf, setbitHdr...)
					buf = appendLen(buf, len(bkey))
					buf = append(buf, crlfB...)
					buf = append(buf, bkey...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(offset))
					buf = append(buf, crlfB...)
					buf = append(buf, offset...)
					buf = append(buf, "\r\n$1\r\n1\r\n"...)
				}
				writeFull(conn, buf)
				skipLines(r, batch) // SETBIT returns :0 or :1
				sent += batch
			}

			// Phase 2: BITCOUNT <key>
			buf = buf[:0]
			buf = append(buf, bitcountHdr...)
			buf = appendLen(buf, len(bkey))
			buf = append(buf, crlfB...)
			buf = append(buf, bkey...)
			buf = append(buf, crlfB...)
			writeFull(conn, buf)
			skipLines(r, 1)
		})
	}
	wg.Wait()
	recordResult("SETBIT/BITCOUNT (bitmap)", total, time.Since(start))
}

// HyperLogLog: each client adds MIX_OPS_CLIENT distinct elements to its own
// key, then reads the count. Ops counted = PFADD calls.

var (
	pfaddHdr   = []byte("*3\r\n$5\r\nPFADD\r\n$")
	pfcountHdr = []byte("*2\r\n$7\r\nPFCOUNT\r\n$")
)

func runBenchHLL() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*56)
			var kb, mb [32]byte
			hkey := clientKey("hll:", i, kb[:])

			// Phase 1: PFADD <key> <element>
			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					elem := appendLen(mb[:0], sent+j)
					buf = append(buf, pfaddHdr...)
					buf = appendLen(buf, len(hkey))
					buf = append(buf, crlfB...)
					buf = append(buf, hkey...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(elem))
					buf = append(buf, crlfB...)
					buf = append(buf, elem...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch) // PFADD returns :0 or :1
				sent += batch
			}

			// Phase 2: PFCOUNT <key>
			buf = buf[:0]
			buf = append(buf, pfcountHdr...)
			buf = appendLen(buf, len(hkey))
			buf = append(buf, crlfB...)
			buf = append(buf, hkey...)
			buf = append(buf, crlfB...)
			writeFull(conn, buf)
			skipLines(r, 1)
		})
	}
	wg.Wait()
	recordResult("PFADD/PFCOUNT (HyperLogLog)", total, time.Since(start))
}

// Geospatial: each client adds MIX_OPS_CLIENT members with synthetic (lon, lat)
// coords, then queries GEODIST between two members. Ops counted = GEOADD calls.

var geoadd5Hdr = []byte("*5\r\n$6\r\nGEOADD\r\n$")

func runBenchGeo() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*96)
			var kb, mb [32]byte
			gkey := clientKey("geo:", i, kb[:])

			// Phase 1: GEOADD <key> <lon> <lat> <member>
			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					idx := sent + j
					lon := float64(idx%360) - 180.0
					lat := float64(idx%170) - 85.0
					lonStr := strconv.AppendFloat(nil, lon, 'f', 4, 64)
					latStr := strconv.AppendFloat(nil, lat, 'f', 4, 64)
					member := appendLen(mb[:0], idx)

					buf = append(buf, geoadd5Hdr...)
					buf = appendLen(buf, len(gkey))
					buf = append(buf, crlfB...)
					buf = append(buf, gkey...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(lonStr))
					buf = append(buf, crlfB...)
					buf = append(buf, lonStr...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(latStr))
					buf = append(buf, crlfB...)
					buf = append(buf, latStr...)
					buf = append(buf, "\r\n$"...)
					buf = appendLen(buf, len(member))
					buf = append(buf, crlfB...)
					buf = append(buf, member...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch) // GEOADD returns :N
				sent += batch
			}

			// Phase 2: GEODIST <key> 0 1 km
			mem0 := appendLen(mb[:0], 0)
			mem1 := appendLen(mb[:0], 1)
			buf = buf[:0]
			buf = append(buf, "*5\r\n$7\r\nGEODIST\r\n$"...)
			buf = appendLen(buf, len(gkey))
			buf = append(buf, crlfB...)
			buf = append(buf, gkey...)
			buf = append(buf, "\r\n$"...)
			buf = appendLen(buf, len(mem0))
			buf = append(buf, crlfB...)
			buf = append(buf, mem0...)
			buf = append(buf, "\r\n$"...)
			buf = appendLen(buf, len(mem1))
			buf = append(buf, crlfB...)
			buf = append(buf, mem1...)
			buf = append(buf, "\r\n$2\r\nkm\r\n"...)
			writeFull(conn, buf)
			skipGetReplies(r, 1) // GEODIST returns bulk string or nil
		})
	}
	wg.Wait()
	recordResult("GEOADD/GEODIST (geospatial)", total, time.Since(start))
}
