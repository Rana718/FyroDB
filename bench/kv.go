package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

const seqBatch = 64
const clusterPipeSize = 2500

// crc16Table is the exact table Redis uses for hash slot computation (CRC-16/CCITT).
// Source: https://github.com/redis/redis/blob/unstable/src/crc16.c
var crc16Table = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x4864, 0x5845, 0x6826, 0x7807, 0x08e0, 0x18c1, 0x28a2, 0x3883,
	0xc96c, 0xd94d, 0xe92e, 0xf90f, 0x89e8, 0x99c9, 0xa9aa, 0xb98b,
	0x5a55, 0x4a74, 0x7a17, 0x6a36, 0x1ad1, 0x0af0, 0x3a93, 0x2ab2,
	0xdb5d, 0xcb7c, 0xfb1f, 0xeb3e, 0x9bd9, 0x8bf8, 0xbb9b, 0xabba,
	0x6c26, 0x7c07, 0x4c64, 0x5c45, 0x2ca2, 0x3c83, 0x0ce0, 0x1cc1,
	0xed2e, 0xfd0f, 0xcd6c, 0xdd4d, 0xadaa, 0xbd8b, 0x8de8, 0x9dc9,
	0x7e17, 0x6e36, 0x5e55, 0x4e74, 0x3e93, 0x2eb2, 0x1ed1, 0x0ef0,
	0xff0f, 0xef2e, 0xdf4d, 0xcf6c, 0xbfab, 0xaf8a, 0x9fe9, 0x8fc8,
	0x9069, 0x8048, 0xb02b, 0xa00a, 0xd0ed, 0xc0cc, 0xf0af, 0xe08e,
	0x1161, 0x0140, 0x3123, 0x2102, 0x51e5, 0x41c4, 0x71a7, 0x61a6,
	0x9869, 0x8848, 0xb82b, 0xa80a, 0xd8ed, 0xc8cc, 0xf8af, 0xe88e,
	0x1961, 0x0940, 0x3923, 0x2902, 0x59e5, 0x49c4, 0x79a7, 0x69a6,
	0xd068, 0xc049, 0xf02a, 0xe00b, 0x90ec, 0x80cd, 0xb0ae, 0xa08f,
	0x5160, 0x4141, 0x7122, 0x6103, 0x11e4, 0x01c5, 0x31a6, 0x2187,
	0xe968, 0xf949, 0xc92a, 0xd90b, 0xa9ec, 0xb9cd, 0x89ae, 0x998f,
	0x5860, 0x4841, 0x7822, 0x6803, 0x18e4, 0x08c5, 0x38a6, 0x2887,
	0xf36c, 0xe34d, 0xd32e, 0xc30f, 0xb3e8, 0xa3c9, 0x93aa, 0x838b,
	0x7264, 0x6245, 0x5226, 0x4207, 0x32e0, 0x22c1, 0x12a2, 0x0283,
	0xea6c, 0xfa4d, 0xca2e, 0xda0f, 0xaae8, 0xbac9, 0x8aaa, 0x9a8b,
	0x7b64, 0x6b45, 0x5b26, 0x4b07, 0x3be0, 0x2bc1, 0x1ba2, 0x0b83,
	0xfc6c, 0xec4d, 0xdc2e, 0xcc0f, 0xbce8, 0xacc9, 0x9caa, 0x8c8b,
	0x7d64, 0x6d45, 0x5d26, 0x4d07, 0x3de0, 0x2dc1, 0x1da2, 0x0d83,
	0xee6c, 0xfe4d, 0xce2e, 0xde0f, 0xaee8, 0xbec9, 0x8eaa, 0x9e8b,
	0x6f64, 0x7f45, 0x4f26, 0x5f07, 0x2fe0, 0x3fc1, 0x0fa2, 0x1f83,
}

// keySlot computes the Redis cluster hash slot for a key (0–16383).
func keySlot(key []byte) int {
	// Support hash tags: if key contains {...}, hash only the content inside.
	if s := hashTag(key); s != nil {
		key = s
	}
	crc := uint16(0)
	for _, b := range key {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^b]
	}
	return int(crc) % 16384
}

// hashTag extracts the content between the first '{' and the first '}' after it,
// returning nil if no valid tag is present (matching Redis behaviour).
func hashTag(key []byte) []byte {
	for i, b := range key {
		if b == '{' {
			for j := i + 1; j < len(key); j++ {
				if key[j] == '}' && j > i+1 {
					return key[i+1 : j]
				}
			}
		}
	}
	return nil
}

// nodeForKey maps a key to the index in addrs that owns its slot.
// Assumes the cluster evenly distributes the 16384 slots across masters,
// which is exactly what redis-cli --cluster create does by default.
func nodeForKey(key []byte) int {
	if len(addrs) == 1 {
		return 0
	}
	return keySlot(key) * len(addrs) / 16384
}

// ── Connection management ────────────────────────────────────────────────────

// clusterConns[clientID][nodeID] → *net.TCPConn
type clusterConns [][]*net.TCPConn

func preDialCluster(nClients int) clusterConns {
	cc := make(clusterConns, nClients)
	var wg sync.WaitGroup
	for i := 0; i < nClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cc[id] = make([]*net.TCPConn, len(addrs))
			for n, addr := range addrs {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					panic(err)
				}
				tc := c.(*net.TCPConn)
				tc.SetNoDelay(true)
				tc.SetWriteBuffer(1 << 18)
				tc.SetReadBuffer(1 << 18)
				cc[id][n] = tc
			}
		}(i)
	}
	wg.Wait()
	return cc
}

func closeCluster(cc clusterConns) {
	for _, row := range cc {
		for _, c := range row {
			if c != nil {
				c.Close()
			}
		}
	}
}

// preDial opens n connections, each routed to addrs[i%len(addrs)].
// Used for single-node mode only.
func preDial(n int) []*net.TCPConn {
	conns := make([]*net.TCPConn, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conns[id] = dialTCP(id)
		}(i)
	}
	wg.Wait()
	return conns
}

func preDialTo(n, node int) []*net.TCPConn {
	conns := make([]*net.TCPConn, n)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := net.Dial("tcp", addrs[node])
			if err != nil {
				panic(err)
			}
			tc := c.(*net.TCPConn)
			tc.SetNoDelay(true)
			tc.SetWriteBuffer(1 << 18)
			tc.SetReadBuffer(1 << 18)
			conns[id] = tc
		}(i)
	}
	wg.Wait()
	return conns
}

func closeAll(conns []*net.TCPConn) {
	for _, c := range conns {
		if c != nil {
			c.Close()
		}
	}
}

func dialTCP(id int) *net.TCPConn {
	c, err := net.Dial("tcp", pickAddr(id))
	if err != nil {
		panic(err)
	}
	tc := c.(*net.TCPConn)
	tc.SetNoDelay(true)
	tc.SetWriteBuffer(1 << 18)
	tc.SetReadBuffer(1 << 18)
	return tc
}

// ── Benchmark ────────────────────────────────────────────────────────────────

func runKV() {
	if len(addrs) > 1 {
		runClusterKV()
		return
	}

	flushServer()

	label := addrs[0]
	if len(addrs) > 1 {
		label = fmt.Sprintf("cluster(%d masters)", len(addrs))
	}
	fmt.Printf("── KV Benchmark (%s) ─────────────────────────\n", label)
	fmt.Printf("clients=%d  ops/client=%d  pipeline_size=%d  total=%d\n\n",
		CLIENTS, OPS_CLIENT, PIPE_SIZE, CLIENTS*OPS_CLIENT)

	totalOps := int64(CLIENTS * OPS_CLIENT)

	seqCC := preDialCluster(CLIENTS)
	pipeSetCC := preDialCluster(CLIENTS)
	pipeGetCC := preDialCluster(CLIENTS)
	defer closeCluster(seqCC)
	defer closeCluster(pipeSetCC)
	defer closeCluster(pipeGetCC)

	var wg sync.WaitGroup

	// ── Pipeline-64 SET ──────────────────────────────────────────────────────
	seqStart := time.Now()
	for i := 0; i < CLIENTS; i++ {
		wg.Add(1)
		go func(id int, conns []*net.TCPConn) {
			defer wg.Done()
			readers := makeReaders(conns, 128<<10)

			nodeReqs := makeNodeBufs(len(conns), seqBatch*40)
			nodeCnts := make([]int, len(conns))

			var kb [32]byte
			base := id * OPS_CLIENT
			for sent := 0; sent < OPS_CLIENT; {
				batch := seqBatch
				if OPS_CLIENT-sent < batch {
					batch = OPS_CLIENT - sent
				}
				resetBufs(nodeReqs, nodeCnts)
				for j := 0; j < batch; j++ {
					kn := strconv.AppendInt(kb[:0], int64(base+sent+j), 10)
					n := nodeForKey(kn)
					nodeReqs[n] = appendSetBytes(nodeReqs[n], kn)
					nodeCnts[n]++
				}
				// Write to all nodes first, then read all replies — maximises
				// parallel I/O across nodes instead of serialising them.
				flushAll(conns, nodeReqs, nodeCnts)
				readSetReplies(readers, nodeCnts)
				sent += batch
			}
		}(i, seqCC[i])
	}
	wg.Wait()
	seqElapsed := time.Since(seqStart)
	printResult("Pipeline-64 SET", totalOps, seqElapsed)

	// ── Pipelined SET ────────────────────────────────────────────────────────
	pipeSetStart := time.Now()
	for i := 0; i < CLIENTS; i++ {
		wg.Add(1)
		go func(id int, conns []*net.TCPConn) {
			defer wg.Done()
			readers := makeReaders(conns, 128<<10)

			nodeReqs := makeNodeBufs(len(conns), PIPE_SIZE*40)
			nodeCnts := make([]int, len(conns))

			var kb [32]byte
			base := id * OPS_CLIENT
			for sent := 0; sent < OPS_CLIENT; {
				batch := PIPE_SIZE
				if OPS_CLIENT-sent < batch {
					batch = OPS_CLIENT - sent
				}
				resetBufs(nodeReqs, nodeCnts)
				for j := 0; j < batch; j++ {
					kn := strconv.AppendInt(kb[:0], int64(base+sent+j), 10)
					n := nodeForKey(kn)
					nodeReqs[n] = appendSetBytes(nodeReqs[n], kn)
					nodeCnts[n]++
				}
				flushAll(conns, nodeReqs, nodeCnts)
				readSetReplies(readers, nodeCnts)
				sent += batch
			}
		}(i, pipeSetCC[i])
	}
	wg.Wait()
	pipeSetElapsed := time.Since(pipeSetStart)
	printResult("Pipelined SET", totalOps, pipeSetElapsed)

	// ── Pipelined GET ────────────────────────────────────────────────────────
	// GET the exact same keys that were written in the pipelined SET phase.
	pipeGetStart := time.Now()
	for i := 0; i < CLIENTS; i++ {
		wg.Add(1)
		go func(id int, conns []*net.TCPConn) {
			defer wg.Done()
			readers := makeReaders(conns, 256<<10)

			nodeReqs := makeNodeBufs(len(conns), PIPE_SIZE*32)
			nodeCnts := make([]int, len(conns))

			var kb [32]byte
			base := id * OPS_CLIENT
			for sent := 0; sent < OPS_CLIENT; {
				batch := PIPE_SIZE
				if OPS_CLIENT-sent < batch {
					batch = OPS_CLIENT - sent
				}
				resetBufs(nodeReqs, nodeCnts)
				for j := 0; j < batch; j++ {
					kn := strconv.AppendInt(kb[:0], int64(base+sent+j), 10)
					n := nodeForKey(kn)
					nodeReqs[n] = appendGetBytes(nodeReqs[n], kn)
					nodeCnts[n]++
				}
				flushAll(conns, nodeReqs, nodeCnts)
				readGetReplies(readers, nodeCnts)
				sent += batch
			}
		}(i, pipeGetCC[i])
	}
	wg.Wait()
	pipeGetElapsed := time.Since(pipeGetStart)
	printResult("Pipelined GET", totalOps, pipeGetElapsed)

	seqRate := rate(totalOps, seqElapsed)
	setRate := rate(totalOps, pipeSetElapsed)
	getRate := rate(totalOps, pipeGetElapsed)

	mixResults = append(mixResults,
		benchResult{"Pipeline-64 SET", totalOps, seqElapsed},
		benchResult{"Pipelined SET", totalOps, pipeSetElapsed},
		benchResult{"Pipelined GET", totalOps, pipeGetElapsed},
	)

	fmt.Println("\n── KV Summary ──────────────────────────────────")
	fmt.Printf("pipeline-64 SET:   %s\n", fmtRate(seqRate))
	fmt.Printf("pipelined  SET:    %s\n", fmtRate(setRate))
	fmt.Printf("pipelined  GET:    %s\n", fmtRate(getRate))
	fmt.Printf("pipeline speedup:  %.1fx\n", setRate/seqRate)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func runClusterKV() {
	fmt.Printf("── KV Benchmark (cluster(%d masters)) ────────────────────────\n", len(addrs))
	fmt.Printf("clients=%d  ops/client=%d  pipeline_size=%d  total=%d\n\n",
		CLIENTS, OPS_CLIENT, clusterPipeSize, CLIENTS*OPS_CLIENT)

	totalOps := int64(CLIENTS * OPS_CLIENT)
	tags := clusterHashTags()
	seqConns := preDial(CLIENTS)
	setConns := preDial(CLIENTS)
	getConns := preDial(CLIENTS)
	defer closeAll(seqConns)
	defer closeAll(setConns)
	defer closeAll(getConns)

	seqElapsed := runClusterPhase(seqConns, tags, seqBatch, false)
	printResult("Pipeline-64 SET", totalOps, seqElapsed)
	setElapsed := runClusterPhase(setConns, tags, clusterPipeSize, false)
	printResult("Pipelined SET", totalOps, setElapsed)
	getElapsed := runClusterPhase(getConns, tags, clusterPipeSize, true)
	printResult("Pipelined GET", totalOps, getElapsed)

	seqRate := rate(totalOps, seqElapsed)
	setRate := rate(totalOps, setElapsed)
	getRate := rate(totalOps, getElapsed)

	mixResults = append(mixResults,
		benchResult{"Pipeline-64 SET", totalOps, seqElapsed},
		benchResult{"Pipelined SET", totalOps, setElapsed},
		benchResult{"Pipelined GET", totalOps, getElapsed},
	)

	fmt.Println("\n── KV Summary ─────────────────────────────────")
	fmt.Printf("pipeline-64 SET:   %s\n", fmtRate(seqRate))
	fmt.Printf("pipelined  SET:    %s\n", fmtRate(setRate))
	fmt.Printf("pipelined  GET:    %s\n", fmtRate(getRate))
	fmt.Printf("pipeline speedup:  %.1fx\n", setRate/seqRate)
}

func runClusterPhase(conns []*net.TCPConn, tags [][]byte, batchSize int, get bool) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	for id, conn := range conns {
		wg.Add(1)
		go func(id int, conn *net.TCPConn) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 256<<10)
			requests := make([]byte, 0, batchSize*48)
			tag := tags[id%len(addrs)]
			base := id * OPS_CLIENT
			var kb [64]byte
			for sent := 0; sent < OPS_CLIENT; {
				batch := min(batchSize, OPS_CLIENT-sent)
				requests = requests[:0]
				for j := 0; j < batch; j++ {
					key := append(kb[:0], tag...)
					key = strconv.AppendInt(key, int64(base+sent+j), 10)
					if get {
						requests = appendGetBytes(requests, key)
					} else {
						requests = appendSetBytes(requests, key)
					}
				}
				writeFull(conn, requests)
				if get {
					skipGetReplies(r, batch)
				} else {
					discardN(r, batch*5)
				}
				sent += batch
			}
		}(id, conn)
	}
	wg.Wait()
	return time.Since(start)
}

func clusterHashTags() [][]byte {
	tags := make([][]byte, len(addrs))
	remaining := len(tags)
	var candidate [32]byte
	for i := 0; remaining > 0; i++ {
		raw := strconv.AppendInt(candidate[:0], int64(i), 10)
		node := nodeForKey(raw)
		if tags[node] != nil {
			continue
		}
		tag := make([]byte, 0, len(raw)+3)
		tag = append(tag, '{')
		tag = append(tag, raw...)
		tag = append(tag, '}', ':')
		tags[node] = tag
		remaining--
	}
	return tags
}

func makeReaders(conns []*net.TCPConn, size int) []*bufio.Reader {
	rs := make([]*bufio.Reader, len(conns))
	for i, c := range conns {
		rs[i] = bufio.NewReaderSize(c, size)
	}
	return rs
}

func makeNodeBufs(n, cap int) [][]byte {
	bufs := make([][]byte, n)
	for i := range bufs {
		bufs[i] = make([]byte, 0, cap)
	}
	return bufs
}

func resetBufs(bufs [][]byte, cnts []int) {
	for i := range bufs {
		bufs[i] = bufs[i][:0]
		cnts[i] = 0
	}
}

// flushAll writes to every node that has pending data.
func flushAll(conns []*net.TCPConn, bufs [][]byte, cnts []int) {
	for n, cnt := range cnts {
		if cnt > 0 {
			writeFull(conns[n], bufs[n])
		}
	}
}

// warmup runs a small pipelined SET/GET round so neither the client nor the
// server pays first-touch costs inside the timed phases.
func warmup() {
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Add(1)
		go func(conn *net.TCPConn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(conn, 32<<10)
			buf := make([]byte, 0, 512)
			var kb [32]byte
			key := strconv.AppendInt(append(kb[:0], "warmup:"...), int64(id), 10)
			for round := 0; round < 4; round++ {
				buf = buf[:0]
				for j := 0; j < 50; j++ {
					buf = appendSetBytes(buf, key)
				}
				writeFull(conn, buf)
				discardN(r, 50*5)
				buf = buf[:0]
				for j := 0; j < 50; j++ {
					buf = appendGetBytes(buf, key)
				}
				writeFull(conn, buf)
				skipGetReplies(r, 50)
			}
		}(conns[i], i)
	}
	wg.Wait()
	flushServer()
}

// readSetReplies reads cnt[n] × "+OK\r\n" (5 bytes each) from each node's reader.
func readSetReplies(readers []*bufio.Reader, cnts []int) {
	for n, cnt := range cnts {
		if cnt > 0 {
			discardN(readers[n], cnt*5)
		}
	}
}

// readGetReplies reads cnt[n] GET replies from each node's reader.
func readGetReplies(readers []*bufio.Reader, cnts []int) {
	for n, cnt := range cnts {
		if cnt > 0 {
			skipGetReplies(readers[n], cnt)
		}
	}
}

var (
	setHdr  = []byte("*3\r\n$3\r\nSET\r\n$")
	getHdr  = []byte("*2\r\n$3\r\nGET\r\n$")
	valPart = []byte("\r\n$5\r\nvalue\r\n")
	crlfB   = []byte("\r\n")
)

func appendSetBytes(out, key []byte) []byte {
	out = append(out, setHdr...)
	out = appendLen(out, len(key))
	out = append(out, crlfB...)
	out = append(out, key...)
	return append(out, valPart...)
}

func appendGetBytes(out, key []byte) []byte {
	out = append(out, getHdr...)
	out = appendLen(out, len(key))
	out = append(out, crlfB...)
	out = append(out, key...)
	return append(out, crlfB...)
}

func appendLen(out []byte, n int) []byte {
	if n < 10 {
		return append(out, byte('0'+n))
	}
	var buf [5]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return append(out, buf[pos:]...)
}

func writeFull(conn *net.TCPConn, p []byte) {
	for len(p) != 0 {
		n, err := conn.Write(p)
		if err != nil {
			panic(err)
		}
		if n == 0 {
			panic("tcp write made no progress")
		}
		p = p[n:]
	}
}

func discardN(r *bufio.Reader, n int) {
	for n > 0 {
		d, err := r.Discard(n)
		n -= d
		if err != nil {
			panic(err)
		}
		if d == 0 {
			panic("RESP reader made no progress")
		}
	}
}

func skipGetReplies(r *bufio.Reader, n int) {
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
			if len(line) >= 2 && line[0] == '-' {
				continue
			}
			vlen := 0
			for _, c := range line {
				if c >= '0' && c <= '9' {
					vlen = vlen*10 + int(c-'0')
				} else {
					break
				}
			}
			discardN(r, vlen+2) // value + trailing \r\n
		default:
			isError := b == '-'
			for {
				line, e := r.ReadSlice('\n')
				if e != nil && e != bufio.ErrBufferFull {
					panic(e)
				}
				if isError {
					panic("Redis error reply: " + string(line))
				}
				if e != bufio.ErrBufferFull {
					break
				}
			}
		}
	}
}

func skipLines(r *bufio.Reader, n int) {
	for i := 0; i < n; i++ {
		first := true
		for {
			line, err := r.ReadSlice('\n')
			if err != nil && err != bufio.ErrBufferFull {
				panic(err)
			}
			if first && len(line) > 0 && line[0] == '-' {
				panic("Redis error reply: " + string(line))
			}
			first = false
			if err != bufio.ErrBufferFull {
				break
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func rate(ops int64, d time.Duration) float64 {
	return float64(ops) / d.Seconds()
}

func fmtRate(r float64) string {
	switch {
	case r >= 1_000_000:
		return fmt.Sprintf("%.2fM ops/sec", r/1_000_000)
	case r >= 1_000:
		return fmt.Sprintf("%.1fk ops/sec", r/1_000)
	default:
		return fmt.Sprintf("%.0f ops/sec", r)
	}
}

func printResult(label string, ops int64, d time.Duration) {
	fmt.Printf("── %s\n", label)
	fmt.Printf("   ops:     %d\n", ops)
	fmt.Printf("   elapsed: %s\n", d.Round(time.Millisecond))
	fmt.Printf("   ops/sec: %s\n\n", fmtRate(rate(ops, d)))
}
