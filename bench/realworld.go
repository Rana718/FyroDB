package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MIX_OPS_CLIENT = 10000
	MIX_PIPE       = 100
	MIX_HOTKEY_OPS = 50000
	MIX_QUEUE_OPS  = 10000
)

var (
	// skipFlush (-f): keep all keys between phases so peak RSS at the end
	// reflects the true steady-state footprint of the full workload.
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 256<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb [32]byte
			base := id * MIX_OPS_CLIENT

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for j := 0; j < sets; j++ {
					kn := strconv.AppendInt(append(kb[:0], "mix:"...), int64(base+(sent/2)+j), 10)
					buf = appendSetBytes(buf, kn)
				}
				for j := 0; j < gets; j++ {
					kn := strconv.AppendInt(append(kb[:0], "mix:"...), int64(base+(sent/2)+j), 10)
					buf = appendGetBytes(buf, kn)
				}
				writeFull(conn, buf)
				discardN(r, sets*5)
				skipGetReplies(r, gets)
				sent += batch
			}
		}(conns[i], i)
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
	for id, nodes := range conns {
		wg.Add(1)
		go func(id int, conns []*net.TCPConn) {
			defer wg.Done()
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
				for j := 0; j < half; j++ {
					key := append(kb[:0], tags[id%len(tags)]...)
					key = strconv.AppendInt(key, int64(base+sent/2+j), 10)
					n := nodeForKey(key)
					bufs[n] = appendSetBytes(bufs[n], key)
					cnts[n]++
					sets[n]++
				}
				for j := 0; j < batch/2; j++ {
					key := append(kb[:0], tags[id%len(tags)]...)
					key = strconv.AppendInt(key, int64(base+sent/2+j), 10)
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
		}(id, nodes)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*28)
			var kb [32]byte
			key := clientKey("incr:", id%100, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb [32]byte
			key := clientKey("sess:", id, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb [32]byte
			qkey := clientKey("q:", id, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*48)
			var kb, mb [32]byte
			setkey := clientKey("s:", id, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
					member := strconv.AppendInt(mb[:0], int64(sent+j), 10)
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*64)
			var kb, mb [32]byte
			lbkey := clientKey("lb:", id, kb[:])

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
					member := strconv.AppendInt(mb[:0], int64(sent+j), 10)
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*80)
			var kb [32]byte
			base := id * MIX_OPS_CLIENT
			prefix := []byte(nil)
			if len(addrs) > 1 {
				prefix = clusterHashTags()[id%len(addrs)]
			}

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
					kn := strconv.AppendInt(append(kb[:0], prefix...), int64(base+sent+j), 10)
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
		}(conns[i], i)
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*32)

			for sent := 0; sent < MIX_HOTKEY_OPS; {
				batch := MIX_PIPE
				if MIX_HOTKEY_OPS-sent < batch {
					batch = MIX_HOTKEY_OPS - sent
				}
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for j := 0; j < sets; j++ {
					buf = appendSetBytes(buf, hotkey)
				}
				for j := 0; j < gets; j++ {
					buf = appendGetBytes(buf, hotkey)
				}
				writeFull(conn, buf)
				discardN(r, sets*5)
				skipGetReplies(r, gets)
				sent += batch
			}
		}(conns[i])
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

	for i := 0; i < half; i++ {
		wg.Add(1)
		go func(conn *net.TCPConn) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*40)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := MIX_PIPE
				if MIX_QUEUE_OPS-sent < batch {
					batch = MIX_QUEUE_OPS - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
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
		}(producers[i])
	}

	for i := 0; i < half; i++ {
		wg.Add(1)
		go func(conn *net.TCPConn) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 64<<10)
			buf := make([]byte, 0, MIX_PIPE*28)

			for sent := 0; sent < MIX_QUEUE_OPS; {
				batch := MIX_PIPE
				if MIX_QUEUE_OPS-sent < batch {
					batch = MIX_QUEUE_OPS - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
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
		}(consumers[i])
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
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 128<<10)
			buf := make([]byte, 0, MIX_PIPE*96)
			var kb [32]byte
			base := id * MIX_OPS_CLIENT
			prefix := []byte("json:")
			if len(addrs) > 1 {
				prefix = append(append([]byte(nil), clusterHashTags()[id%len(addrs)]...), prefix...)
			}

			for sent := 0; sent < MIX_OPS_CLIENT; {
				batch := MIX_PIPE
				if MIX_OPS_CLIENT-sent < batch {
					batch = MIX_OPS_CLIENT - sent
				}
				buf = buf[:0]
				sets := (batch + 1) / 2
				gets := batch / 2
				for j := 0; j < sets; j++ {
					kn := strconv.AppendInt(append(kb[:0], prefix...), int64(base+(sent/2)+j), 10)
					buf = append(buf, jsonSetHdr...)
					buf = appendLen(buf, len(kn))
					buf = append(buf, crlfB...)
					buf = append(buf, kn...)
					buf = append(buf, pathDotVal...)
				}
				for j := 0; j < gets; j++ {
					kn := strconv.AppendInt(append(kb[:0], prefix...), int64(base+(sent/2)+j), 10)
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
		}(conns[i], i)
	}
	wg.Wait()
	recordResult("JSON.SET/GET (documents)", total, time.Since(start))
}
