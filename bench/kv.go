package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

const seqBatch = 64
const clusterPipeSize = 2500

// CRC-16/CCITT table Redis uses for hash slots.
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
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

// keySlot computes the Redis cluster hash slot for a key (0–16383).
func keySlot(key []byte) int {
	// Hash tags: if key contains {...}, hash only the content inside.
	if s := hashTag(key); s != nil {
		key = s
	}
	crc := uint16(0)
	for _, b := range key {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^b]
	}
	return int(crc) % 16384
}

// hashTag returns the content between the first '{' and a following '}',
// or nil if there is no valid tag (matching Redis behaviour).
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

// nodeForKey maps a key to the owning index in addrs, assuming slots are
// spread evenly across masters (the redis-cli --cluster create default).
func nodeForKey(key []byte) int {
	if len(addrs) == 1 {
		return 0
	}
	return keySlot(key) * len(addrs) / 16384
}

// clusterConns[clientID][nodeID] → *net.TCPConn
type clusterConns [][]*net.TCPConn

func preDialCluster(nClients int) clusterConns {
	cc := make(clusterConns, nClients)
	var wg sync.WaitGroup
	for i := range nClients {
		wg.Go(func() {
			cc[i] = make([]*net.TCPConn, len(addrs))
			for n, addr := range addrs {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					panic(err)
				}
				tc := c.(*net.TCPConn)
				tc.SetNoDelay(true)
				tc.SetWriteBuffer(1 << 20)
				tc.SetReadBuffer(1 << 20)
				cc[i][n] = tc
			}
		})
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

// preDial opens n connections, routed to addrs[i%len(addrs)] (single-node only).
func preDial(n int) []*net.TCPConn {
	conns := make([]*net.TCPConn, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			conns[i] = dialTCP(i)
		})
	}
	wg.Wait()
	return conns
}

func preDialTo(n, node int) []*net.TCPConn {
	conns := make([]*net.TCPConn, n)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Go(func() {
			c, err := net.Dial("tcp", addrs[node])
			if err != nil {
				panic(err)
			}
			tc := c.(*net.TCPConn)
			tc.SetNoDelay(true)
			tc.SetWriteBuffer(1 << 20)
			tc.SetReadBuffer(1 << 20)
			conns[i] = tc
		})
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
	tc.SetWriteBuffer(1 << 20)
	tc.SetReadBuffer(1 << 20)
	return tc
}

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

	seqConns := preDial(CLIENTS)
	defer closeAll(seqConns)

	var wg sync.WaitGroup

	seqStart := time.Now()
	for i := range CLIENTS {
		wg.Go(func() {
			conn := seqConns[i]
			r := bufio.NewReaderSize(conn, 256<<10)
			buf := make([]byte, 0, seqBatch*48)
			base := i * OPS_CLIENT
			for sent := 0; sent < OPS_CLIENT; {
				batch := min(seqBatch, OPS_CLIENT-sent)
				buf = buildBatchFastSet(base+sent, batch, buf[:0])
				writeFull(conn, buf)
				discardN(r, batch*5)
				sent += batch
			}
		})
	}
	wg.Wait()
	seqElapsed := time.Since(seqStart)
	printResult("Pipeline-64 SET", totalOps, seqElapsed)

	pipeSetConns := preDial(CLIENTS)
	defer closeAll(pipeSetConns)

	pipeSetStart := time.Now()
	for i := range CLIENTS {
		wg.Go(func() {
			base := i * OPS_CLIENT
			runAsyncPipeline(pipeSetConns[i], OPS_CLIENT,
				func(offset, count int, out []byte) []byte {
					return buildBatchFastSet(base+offset, count, out)
				}, 5) // "+OK\r\n" = 5 bytes
		})
	}
	wg.Wait()
	pipeSetElapsed := time.Since(pipeSetStart)
	printResult("Pipelined SET", totalOps, pipeSetElapsed)

	// GET the same keys written in the pipelined SET phase.
	pipeGetConns := preDial(CLIENTS)
	defer closeAll(pipeGetConns)

	pipeGetStart := time.Now()
	for i := range CLIENTS {
		wg.Go(func() {
			base := i * OPS_CLIENT
			runAsyncPipeline(pipeGetConns[i], OPS_CLIENT,
				func(offset, count int, out []byte) []byte {
					return buildBatchFastGet(base+offset, count, out)
				}, 11) // "$5\r\nvalue\r\n" = 11 bytes
		})
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
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 256<<10)
			requests := make([]byte, 0, batchSize*48)
			tag := tags[id%len(addrs)]
			base := id * OPS_CLIENT
			var kb [64]byte
			for sent := 0; sent < OPS_CLIENT; {
				batch := min(batchSize, OPS_CLIENT-sent)
				requests = requests[:0]
				for j := range batch {
					key := appendLen(append(kb[:0], tag...), base+sent+j)
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
		})
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

// warmup runs a small pipelined SET/GET round so neither client nor server
// pays first-touch costs inside the timed phases.
func warmup() {
	if len(addrs) > 1 {
		warmupCluster()
		return
	}
	conns := preDial(CLIENTS)
	defer closeAll(conns)
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(conn, 32<<10)
			buf := make([]byte, 0, 512)
			var kb [32]byte
			key := appendLen(append(kb[:0], "warmup:"...), i)
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
		})
	}
	wg.Wait()
	flushServer()
}

// Cluster warmup: route each client's key only to its owning node.
func warmupCluster() {
	conns := preDialCluster(CLIENTS)
	defer closeCluster(conns)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Go(func() {
			nodeConns := conns[i]
			readers := makeReaders(nodeConns, 32<<10)
			bufs := makeNodeBufs(len(nodeConns), 512)
			var kb [64]byte
			key := appendLen(append(kb[:0], "warmup:"...), i)
			n := nodeForKey(key)
			for round := 0; round < 4; round++ {
				bufs[n] = appendSetBytes(bufs[n][:0], key)
				for j := 1; j < 50; j++ {
					bufs[n] = appendSetBytes(bufs[n], key)
				}
				writeFull(nodeConns[n], bufs[n])
				discardN(readers[n], 50*5)
				bufs[n] = appendGetBytes(bufs[n][:0], key)
				for j := 1; j < 50; j++ {
					bufs[n] = appendGetBytes(bufs[n], key)
				}
				writeFull(nodeConns[n], bufs[n])
				skipGetReplies(readers[n], 50)
			}
		})
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

const (
	fastGetFrame   = "$5\r\nvalue\r\n"
	fast16GetFrame = "$16\r\n0123456789abcdef\r\n"
	fastPopFrame   = "$7\r\npayload\r\n"
	fastHgetFrame  = "$1\r\nv\r\n"
	fastNilFrame   = "$-1\r\n"
	fastOneLine    = ":1\r\n"
	fastOKLine     = "+OK\r\n"
)

var (
	fastGetFrameB   = []byte(fastGetFrame)
	fast16GetFrameB = []byte(fast16GetFrame)
	fastPopFrameB   = []byte(fastPopFrame)
	fastHgetFrameB  = []byte(fastHgetFrame)
	fastNilFrameB   = []byte(fastNilFrame)
	fastOneLineB    = []byte(fastOneLine)
	fastOKLineB     = []byte(fastOKLine)
)

var setHdrDigits = [11][]byte{
	0: nil,
	1: []byte("*3\r\n$3\r\nSET\r\n$1\r\n"),
	2: []byte("*3\r\n$3\r\nSET\r\n$2\r\n"),
	3: []byte("*3\r\n$3\r\nSET\r\n$3\r\n"),
	4: []byte("*3\r\n$3\r\nSET\r\n$4\r\n"),
	5: []byte("*3\r\n$3\r\nSET\r\n$5\r\n"),
	6: []byte("*3\r\n$3\r\nSET\r\n$6\r\n"),
	7: []byte("*3\r\n$3\r\nSET\r\n$7\r\n"),
	8: []byte("*3\r\n$3\r\nSET\r\n$8\r\n"),
	9: []byte("*3\r\n$3\r\nSET\r\n$9\r\n"),
	10: []byte("*3\r\n$3\r\nSET\r\n$10\r\n"),
}

var getHdrDigits = [11][]byte{
	0: nil,
	1: []byte("*2\r\n$3\r\nGET\r\n$1\r\n"),
	2: []byte("*2\r\n$3\r\nGET\r\n$2\r\n"),
	3: []byte("*2\r\n$3\r\nGET\r\n$3\r\n"),
	4: []byte("*2\r\n$3\r\nGET\r\n$4\r\n"),
	5: []byte("*2\r\n$3\r\nGET\r\n$5\r\n"),
	6: []byte("*2\r\n$3\r\nGET\r\n$6\r\n"),
	7: []byte("*2\r\n$3\r\nGET\r\n$7\r\n"),
	8: []byte("*2\r\n$3\r\nGET\r\n$8\r\n"),
	9: []byte("*2\r\n$3\r\nGET\r\n$9\r\n"),
	10: []byte("*2\r\n$3\r\nGET\r\n$10\r\n"),
}

const digits100 = "" +
	"00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

type hdrWords struct {
	w0   uint64
	w1   uint64
	tail byte
	len  int
}

var setHdrWords [11]hdrWords
var getHdrWords [11]hdrWords

var valPartW0 uint64
var valPartW1 uint64

func init() {
	if len(valPart) >= 13 {
		valPartW0 = binary.LittleEndian.Uint64(valPart[0:8])
		valPartW1 = binary.LittleEndian.Uint64(valPart[len(valPart)-8:])
	}
	for d := 1; d <= 10; d++ {
		sh := setHdrDigits[d]
		if len(sh) >= 16 {
			setHdrWords[d].w0 = binary.LittleEndian.Uint64(sh[0:8])
			setHdrWords[d].w1 = binary.LittleEndian.Uint64(sh[8:16])
			if len(sh) > 16 {
				setHdrWords[d].tail = sh[16]
			}
			setHdrWords[d].len = len(sh)
		}
		gh := getHdrDigits[d]
		if len(gh) >= 16 {
			getHdrWords[d].w0 = binary.LittleEndian.Uint64(gh[0:8])
			getHdrWords[d].w1 = binary.LittleEndian.Uint64(gh[8:16])
			if len(gh) > 16 {
				getHdrWords[d].tail = gh[16]
			}
			getHdrWords[d].len = len(gh)
		}
	}
}

func fastIntDigits(n int) int {
	if n < 10 {
		return 1
	}
	if n < 100 {
		return 2
	}
	if n < 1000 {
		return 3
	}
	if n < 10000 {
		return 4
	}
	if n < 100000 {
		return 5
	}
	if n < 1000000 {
		return 6
	}
	if n < 10000000 {
		return 7
	}
	if n < 100000000 {
		return 8
	}
	if n < 1000000000 {
		return 9
	}
	return 10
}

func buildBatchFastSet(base, count int, buf []byte) []byte {
	out := buf
	valLen := len(valPart)
	valOffset := valLen - 8
	for j := 0; j < count; j++ {
		n := base + j
		d := fastIntDigits(n)
		hw := &setHdrWords[d]
		pos := len(out)
		tot := hw.len + d + valLen
		out = out[:pos+tot]

		binary.LittleEndian.PutUint64(out[pos:], hw.w0)
		binary.LittleEndian.PutUint64(out[pos+8:], hw.w1)
		if hw.len > 16 {
			out[pos+16] = hw.tail
		}

		kStart := pos + hw.len
		kEnd := kStart + d
		curr := n
		idx := kEnd
		for curr >= 100 {
			q := curr / 100
			r := (curr - q*100) * 2
			curr = q
			idx -= 2
			out[idx] = digits100[r]
			out[idx+1] = digits100[r+1]
		}
		if curr < 10 {
			out[kStart] = byte('0' + curr)
		} else {
			r := curr * 2
			out[kStart] = digits100[r]
			out[kStart+1] = digits100[r+1]
		}

		binary.LittleEndian.PutUint64(out[kEnd:], valPartW0)
		binary.LittleEndian.PutUint64(out[kEnd+valOffset:], valPartW1)
	}
	return out
}

func buildBatchFastGet(base, count int, buf []byte) []byte {
	out := buf
	for j := 0; j < count; j++ {
		n := base + j
		d := fastIntDigits(n)
		hw := &getHdrWords[d]
		pos := len(out)
		tot := hw.len + d + 2
		out = out[:pos+tot]

		binary.LittleEndian.PutUint64(out[pos:], hw.w0)
		binary.LittleEndian.PutUint64(out[pos+8:], hw.w1)
		if hw.len > 16 {
			out[pos+16] = hw.tail
		}

		kStart := pos + hw.len
		kEnd := kStart + d
		curr := n
		idx := kEnd
		for curr >= 100 {
			q := curr / 100
			r := (curr - q*100) * 2
			curr = q
			idx -= 2
			out[idx] = digits100[r]
			out[idx+1] = digits100[r+1]
		}
		if curr < 10 {
			out[kStart] = byte('0' + curr)
		} else {
			r := curr * 2
			out[kStart] = digits100[r]
			out[kStart+1] = digits100[r+1]
		}
		out[kEnd] = '\r'
		out[kEnd+1] = '\n'
	}
	return out
}

// runAsyncPipeline saturates a connection with independent writer and reader goroutines.
// buildBatch encodes commands; replyBytesPerOp is the byte size per reply for bulk discard.
func runAsyncPipeline(
	conn *net.TCPConn,
	totalOps int,
	buildBatch func(base, count int, out []byte) []byte,
	replyBytesPerOp int,
) {
	const batchSize = 2500
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 0, batchSize*50)
		sent := 0
		for sent < totalOps {
			n := min(batchSize, totalOps-sent)
			buf = buildBatch(sent, n, buf[:0])
			writeFull(conn, buf)
			sent += n
		}
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 128*1024)
		rem := totalOps * replyBytesPerOp
		for rem > 0 {
			toRead := min(len(buf), rem)
			n, err := conn.Read(buf[:toRead])
			if err != nil {
				panic(err)
			}
			rem -= n
		}
	}()

	wg.Wait()
}

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
	var buf [12]byte
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
	for n > 0 {
		avail := r.Buffered()
		if avail == 0 {
			b, err := r.ReadByte()
			if err != nil {
				panic(err)
			}
			if b == '-' {
				line, _ := r.ReadString('\n')
				panic("Redis error reply: " + string(b) + line)
			}
			r.UnreadByte()
			avail = r.Buffered()
		}
		buf, _ := r.Peek(avail)
		repliesInBuf := 0
		pos := 0

		for pos < len(buf) && repliesInBuf < n {
			switch buf[pos] {
			case '$':
				rem := len(buf) - pos
				// Fast path matches for common static frame shapes
				if rem >= len(fastGetFrameB) && bytes.Equal(buf[pos:pos+len(fastGetFrameB)], fastGetFrameB) {
					pos += len(fastGetFrameB)
					repliesInBuf++
					continue
				}
				if rem >= len(fastHgetFrameB) && bytes.Equal(buf[pos:pos+len(fastHgetFrameB)], fastHgetFrameB) {
					pos += len(fastHgetFrameB)
					repliesInBuf++
					continue
				}
				if rem >= len(fastPopFrameB) && bytes.Equal(buf[pos:pos+len(fastPopFrameB)], fastPopFrameB) {
					pos += len(fastPopFrameB)
					repliesInBuf++
					continue
				}
				if rem >= len(fastNilFrameB) && bytes.Equal(buf[pos:pos+len(fastNilFrameB)], fastNilFrameB) {
					pos += len(fastNilFrameB)
					repliesInBuf++
					continue
				}
				if rem >= len(fast16GetFrameB) && bytes.Equal(buf[pos:pos+len(fast16GetFrameB)], fast16GetFrameB) {
					pos += len(fast16GetFrameB)
					repliesInBuf++
					continue
				}

				// General bulk string: $<len>\r\n<data>\r\n
				crlf := bytes.IndexByte(buf[pos:min(pos+32, len(buf))], '\n')
				if crlf < 0 {
					goto flushAndRefill
				}
				hdrLen := crlf + 1
				// Check for null bulk string: $-1\r\n
				if hdrLen == 5 && buf[pos+1] == '-' && buf[pos+2] == '1' {
					pos += 5
					repliesInBuf++
					continue
				}
				vlen := 0
				for k := pos + 1; k < pos+hdrLen-2; k++ {
					c := buf[k]
					if c >= '0' && c <= '9' {
						vlen = vlen*10 + int(c-'0')
					}
				}
				totalLen := hdrLen + vlen + 2
				if rem < totalLen {
					goto flushAndRefill
				}
				pos += totalLen
				repliesInBuf++

			case '+', ':':
				crlf := bytes.IndexByte(buf[pos:], '\n')
				if crlf < 0 {
					goto flushAndRefill
				}
				pos += crlf + 1
				repliesInBuf++

			case '-':
				panic("Redis error: " + string(buf[pos:]))

			default:
				goto flushAndRefill
			}
		}

	flushAndRefill:
		if repliesInBuf > 0 {
			r.Discard(pos)
			n -= repliesInBuf
		} else {
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
					n--
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
				discardN(r, vlen+2)
				n--
			case '+', ':':
				for {
					_, e := r.ReadSlice('\n')
					if e != nil && e != bufio.ErrBufferFull {
						panic(e)
					}
					if e != bufio.ErrBufferFull {
						n--
						break
					}
				}
			case '-':
				line, _ := r.ReadString('\n')
				panic("Redis error reply: " + line)
			default:
				panic("unexpected RESP type byte: " + string([]byte{b}))
			}
		}
	}
}

func skipLines(r *bufio.Reader, n int) {
	for n > 0 {
		avail := r.Buffered()
		if avail == 0 {
			b, err := r.ReadByte()
			if err != nil {
				panic(err)
			}
			if b == '-' {
				line, _ := r.ReadString('\n')
				panic("Redis error reply: " + string(b) + line)
			}
			r.UnreadByte()
			avail = r.Buffered()
		}
		buf, _ := r.Peek(avail)
		linesInBuf := 0
		lastPos := 0
		for i := 0; i < len(buf) && linesInBuf < n; {
			if buf[i] == '-' {
				line, _ := r.ReadString('\n')
				panic("Redis error reply: " + line)
			}
			idx := bytes.IndexByte(buf[i:], '\n')
			if idx < 0 {
				break
			}
			linesInBuf++
			lastPos = i + idx + 1
			i = lastPos
		}
		if linesInBuf > 0 {
			r.Discard(lastPos)
			n -= linesInBuf
		} else {
			line, err := r.ReadSlice('\n')
			if err != nil && err != bufio.ErrBufferFull {
				panic(err)
			}
			if len(line) > 0 && line[0] == '-' {
				panic("Redis error reply: " + string(line))
			}
			if err != bufio.ErrBufferFull {
				n--
			}
		}
	}
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
