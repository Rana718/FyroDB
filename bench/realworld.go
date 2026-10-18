package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MIX_PIPE = 500

var (
	MIX_OPS_CLIENT = 20_000
	MIX_HOTKEY_OPS = 100_000
	MIX_QUEUE_OPS  = 20_000
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

	jsonSetCmdHdr = []byte("*4\r\n$8\r\nJSON.SET\r\n$")
	jsonGetCmdHdr = []byte("*3\r\n$8\r\nJSON.GET\r\n$")
	// $27\r\n + 27-byte doc + \r\n  (path="."; value stored verbatim)
	jsonDocPart  = []byte("\r\n$1\r\n.\r\n$27\r\n{\"name\":\"test\",\"score\":100}\r\n")
	jsonGetPath  = []byte("\r\n$1\r\n.\r\n")
	jsonKeyPfx   = []byte("json:")
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
			r := bufio.NewReaderSize(conn, 2<<20)
			buf := make([]byte, 0, MIX_PIPE*48)
			base := i * MIX_OPS_CLIENT

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				sets := (batch + 1) / 2
				gets := batch / 2
				kBase := base + (sent / 2)
				buf = buildBatchFastSet(kBase, sets, buf[:0])
				buf = buildBatchFastGet(kBase, gets, buf)
				writeFull(conn, buf)
				discardN(r, sets*5+gets*11)
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
			readers := makeReaders(conns, 1<<20)
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
					discardN(readers[n], sets[n]*5+gets[n]*11)
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
			r := bufio.NewReaderSize(conn, 512<<10)
			var kb [32]byte
			key := clientKey("incr:", i%100, kb[:])
			single := make([]byte, 0, len(incrHdr)+len(key)+16)
			single = append(single, incrHdr...)
			single = appendLen(single, len(key))
			single = append(single, crlfB...)
			single = append(single, key...)
			single = append(single, crlfB...)
			batchBuf := bytes.Repeat(single, MIX_PIPE)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				writeFull(conn, batchBuf[:batch*len(single)])
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
			r := bufio.NewReaderSize(conn, 1<<20)
			var kb [32]byte
			key := clientKey("sess:", i, kb[:])

			singleHset := make([]byte, 0, len(hsetHdr)+len(key)+len(fieldSuffix)+8)
			singleHset = append(singleHset, hsetHdr...)
			singleHset = appendLen(singleHset, len(key))
			singleHset = append(singleHset, crlfB...)
			singleHset = append(singleHset, key...)
			singleHset = append(singleHset, fieldSuffix...)

			singleHget := make([]byte, 0, len(hgetHdr)+len(key)+len(fieldGet)+8)
			singleHget = append(singleHget, hgetHdr...)
			singleHget = appendLen(singleHget, len(key))
			singleHget = append(singleHget, crlfB...)
			singleHget = append(singleHget, key...)
			singleHget = append(singleHget, fieldGet...)

			halfPipe := (MIX_PIPE + 1) / 2
			hsetBatch := bytes.Repeat(singleHset, halfPipe)
			hgetBatch := bytes.Repeat(singleHget, halfPipe)
			fullBatch := append(append([]byte(nil), hsetBatch...), hgetBatch...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				hsets := (batch + 1) / 2
				hgets := batch / 2
				totalBytes := hsets*len(singleHset) + hgets*len(singleHget)
				writeFull(conn, fullBatch[:totalBytes])
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
			r := bufio.NewReaderSize(conn, 1<<20)
			var kb [32]byte
			qkey := clientKey("q:", i, kb[:])

			singleLpush := make([]byte, 0, len(lpushHdr)+len(qkey)+len(lpushBody)+8)
			singleLpush = append(singleLpush, lpushHdr...)
			singleLpush = appendLen(singleLpush, len(qkey))
			singleLpush = append(singleLpush, crlfB...)
			singleLpush = append(singleLpush, qkey...)
			singleLpush = append(singleLpush, lpushBody...)

			singleRpop := make([]byte, 0, len(rpopHdr)+len(qkey)+8)
			singleRpop = append(singleRpop, rpopHdr...)
			singleRpop = appendLen(singleRpop, len(qkey))
			singleRpop = append(singleRpop, crlfB...)
			singleRpop = append(singleRpop, qkey...)
			singleRpop = append(singleRpop, crlfB...)

			halfPipe := (MIX_PIPE + 1) / 2
			lpushBatch := bytes.Repeat(singleLpush, halfPipe)
			rpopBatch := bytes.Repeat(singleRpop, halfPipe)
			fullBatch := append(append([]byte(nil), lpushBatch...), rpopBatch...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				pushes := (batch + 1) / 2
				pops := batch / 2
				totalBytes := pushes*len(singleLpush) + pops*len(singleRpop)
				writeFull(conn, fullBatch[:totalBytes])
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
			r := bufio.NewReaderSize(conn, 1<<20)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb, mb [32]byte
			setkey := clientKey("s:", i, kb[:])
			saddPrefix := make([]byte, 0, len(saddHdr)+len(setkey)+16)
			saddPrefix = append(saddPrefix, saddHdr...)
			saddPrefix = appendLen(saddPrefix, len(setkey))
			saddPrefix = append(saddPrefix, crlfB...)
			saddPrefix = append(saddPrefix, setkey...)
			saddPrefix = append(saddPrefix, "\r\n$"...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					member := appendLen(mb[:0], sent+j)
					buf = append(buf, saddPrefix...)
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
			r := bufio.NewReaderSize(conn, 1<<20)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb, mb [32]byte
			lbkey := clientKey("lb:", i, kb[:])
			zaddPrefix := make([]byte, 0, len(zaddHdr)+len(lbkey)+16)
			zaddPrefix = append(zaddPrefix, zaddHdr...)
			zaddPrefix = appendLen(zaddPrefix, len(lbkey))
			zaddPrefix = append(zaddPrefix, crlfB...)
			zaddPrefix = append(zaddPrefix, lbkey...)
			zaddPrefix = append(zaddPrefix, "\r\n$"...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					member := appendLen(mb[:0], sent+j)
					buf = append(buf, zaddPrefix...)
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
			r := bufio.NewReaderSize(conn, 1<<20)
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
			r := bufio.NewReaderSize(conn, 512<<10)
			singleSet := appendSetBytes(nil, hotkey)
			singleGet := appendGetBytes(nil, hotkey)
			halfPipe := (MIX_PIPE + 1) / 2
			setBatch := bytes.Repeat(singleSet, halfPipe)
			getBatch := bytes.Repeat(singleGet, halfPipe)
			fullBatch := append(append([]byte(nil), setBatch...), getBatch...)

			for sent := 0; sent < MIX_HOTKEY_OPS; {
				batch := min(MIX_PIPE, MIX_HOTKEY_OPS-sent)
				sets := (batch + 1) / 2
				gets := batch / 2
				totalBytes := sets*len(singleSet) + gets*len(singleGet)
				writeFull(conn, fullBatch[:totalBytes])
				discardN(r, sets*5+gets*11)
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
			r := bufio.NewReaderSize(conn, 512<<10)
			singleLpush := make([]byte, 0, len(lpushHdr)+len(qkey)+len(lpushBody)+8)
			singleLpush = append(singleLpush, lpushHdr...)
			singleLpush = appendLen(singleLpush, len(qkey))
			singleLpush = append(singleLpush, crlfB...)
			singleLpush = append(singleLpush, qkey...)
			singleLpush = append(singleLpush, lpushBody...)
			lpushBatch := bytes.Repeat(singleLpush, MIX_PIPE)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := min(MIX_PIPE, MIX_QUEUE_OPS-sent)
				writeFull(conn, lpushBatch[:batch*len(singleLpush)])
				skipLines(r, batch)
				sent += batch
			}
		})
	}

	for i := range half {
		wg.Go(func() {
			conn := consumers[i]
			r := bufio.NewReaderSize(conn, 512<<10)
			singleRpop := make([]byte, 0, len(rpopHdr)+len(qkey)+8)
			singleRpop = append(singleRpop, rpopHdr...)
			singleRpop = appendLen(singleRpop, len(qkey))
			singleRpop = append(singleRpop, crlfB...)
			singleRpop = append(singleRpop, qkey...)
			singleRpop = append(singleRpop, crlfB...)
			rpopBatch := bytes.Repeat(singleRpop, MIX_PIPE)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := min(MIX_PIPE, MIX_QUEUE_OPS-sent)
				writeFull(conn, rpopBatch[:batch*len(singleRpop)])
				skipGetReplies(r, batch)
				sent += batch
			}
		})
	}

	wg.Wait()
	recordResult("Producer/Consumer (50+50)", total, time.Since(start))
}

func buildJsonSetBatch(base, count int, out []byte) []byte {
	var kb [32]byte
	for i := range count {
		kn := appendLen(append(kb[:0], jsonKeyPfx...), base+i)
		out = append(out, jsonSetCmdHdr...)
		out = appendLen(out, len(kn))
		out = append(out, crlfB...)
		out = append(out, kn...)
		out = append(out, jsonDocPart...)
	}
	return out
}

func buildJsonGetBatch(base, count int, out []byte) []byte {
	var kb [32]byte
	for i := range count {
		kn := appendLen(append(kb[:0], jsonKeyPfx...), base+i)
		out = append(out, jsonGetCmdHdr...)
		out = appendLen(out, len(kn))
		out = append(out, crlfB...)
		out = append(out, kn...)
		out = append(out, jsonGetPath...)
	}
	return out
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

	var wg sync.WaitGroup

	setStart := time.Now()
	for i := range CLIENTS {
		wg.Go(func() {
			base := i * MIX_OPS_CLIENT
			runAsyncPipeline(conns[i], MIX_OPS_CLIENT,
				func(offset, count int, out []byte) []byte {
					return buildJsonSetBatch(base+offset, count, out)
				}, 5)
		})
	}
	wg.Wait()
	setElapsed := time.Since(setStart)

	getStart := time.Now()
	for i := range CLIENTS {
		wg.Go(func() {
			base := i * MIX_OPS_CLIENT
			runAsyncPipeline(conns[i], MIX_OPS_CLIENT,
				func(offset, count int, out []byte) []byte {
					return buildJsonGetBatch(base+offset, count, out)
				}, 34)
		})
	}
	wg.Wait()
	getElapsed := time.Since(getStart)

	recordResult("JSON.SET (documents)", total, setElapsed)
	recordResult("JSON.GET (documents)", total, getElapsed)
}

// Each client pipelined-appends entries to its own stream key.
var xaddHdr = []byte("*5\r\n$4\r\nXADD\r\n$")

func appendXADD(out, key []byte, seq int) []byte {
	out = append(out, xaddHdr...)
	out = appendLen(out, len(key))
	out = append(out, crlfB...)
	out = append(out, key...)
	out = append(out, "\r\n$1\r\n*\r\n$1\r\nf\r\n$"...)
	d := fastIntDigits(seq)
	out = appendLen(out, d)
	out = append(out, crlfB...)
	out = appendLen(out, seq)
	out = append(out, crlfB...)
	return out
}

// skipBulkReply drains n RESP bulk-string replies ($<len>\r\n<data>\r\n).
func skipBulkReply(r *bufio.Reader, n int) {
	skipGetReplies(r, n)
}

func runBenchStream() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 2<<20)
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
				skipGetReplies(r, batch)
				sent += batch
			}
		})
	}
	wg.Wait()
	recordResult("XADD (streams)", total, time.Since(start))
}

// Bitmap: each client sets MIX_OPS_CLIENT bits on its own key, then runs BITCOUNT.
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
			r := bufio.NewReaderSize(conn, 2<<20)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb, ob [32]byte
			bkey := clientKey("bmap:", i, kb[:])
			bitPrefix := make([]byte, 0, len(setbitHdr)+len(bkey)+16)
			bitPrefix = append(bitPrefix, setbitHdr...)
			bitPrefix = appendLen(bitPrefix, len(bkey))
			bitPrefix = append(bitPrefix, crlfB...)
			bitPrefix = append(bitPrefix, bkey...)
			bitPrefix = append(bitPrefix, "\r\n$"...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					offset := appendLen(ob[:0], sent+j)
					buf = append(buf, bitPrefix...)
					buf = appendLen(buf, len(offset))
					buf = append(buf, crlfB...)
					buf = append(buf, offset...)
					buf = append(buf, "\r\n$1\r\n1\r\n"...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}

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

// HyperLogLog: each client adds MIX_OPS_CLIENT distinct elements to its key, then reads count.
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
			r := bufio.NewReaderSize(conn, 1<<20)
			buf := make([]byte, 0, MIX_PIPE*56)
			var kb, mb [32]byte
			hkey := clientKey("hll:", i, kb[:])
			pfPrefix := make([]byte, 0, len(pfaddHdr)+len(hkey)+16)
			pfPrefix = append(pfPrefix, pfaddHdr...)
			pfPrefix = appendLen(pfPrefix, len(hkey))
			pfPrefix = append(pfPrefix, crlfB...)
			pfPrefix = append(pfPrefix, hkey...)
			pfPrefix = append(pfPrefix, "\r\n$"...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					elem := appendLen(mb[:0], sent+j)
					buf = append(buf, pfPrefix...)
					buf = appendLen(buf, len(elem))
					buf = append(buf, crlfB...)
					buf = append(buf, elem...)
					buf = append(buf, crlfB...)
				}
				writeFull(conn, buf)
				skipLines(r, batch)
				sent += batch
			}

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

// Geospatial: each client adds members with synthetic coordinates, then queries GEODIST.
var geoadd5Hdr = []byte("*5\r\n$6\r\nGEOADD\r\n$")

func runBenchGeo() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	total := int64(CLIENTS * MIX_OPS_CLIENT)

	start := time.Now()
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 1<<20)
			buf := make([]byte, 0, MIX_PIPE*96)
			var kb, mb, lonBuf, latBuf [32]byte
			gkey := clientKey("geo:", i, kb[:])
			geoPrefix := make([]byte, 0, len(geoadd5Hdr)+len(gkey)+16)
			geoPrefix = append(geoPrefix, geoadd5Hdr...)
			geoPrefix = appendLen(geoPrefix, len(gkey))
			geoPrefix = append(geoPrefix, crlfB...)
			geoPrefix = append(geoPrefix, gkey...)
			geoPrefix = append(geoPrefix, "\r\n$"...)

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := min(MIX_PIPE, MIX_OPS_CLIENT-sent)
				buf = buf[:0]
				for j := range batch {
					idx := sent + j
					lon := float64(idx%360) - 180.0
					lat := float64(idx%170) - 85.0
					lonStr := strconv.AppendFloat(lonBuf[:0], lon, 'f', 4, 64)
					latStr := strconv.AppendFloat(latBuf[:0], lat, 'f', 4, 64)
					member := appendLen(mb[:0], idx)

					buf = append(buf, geoPrefix...)
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
				skipLines(r, batch)
				sent += batch
			}

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
			skipGetReplies(r, 1)
		})
	}
	wg.Wait()
	recordResult("GEOADD/GEODIST (geospatial)", total, time.Since(start))
}
