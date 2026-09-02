// Queue-scaling probe: isolates WHY the Producer/Consumer phase doesn't
// scale with cores. Three configurations over the same server:
//
//   mode=single   one connection, interleaved LPUSH+RPOP on one key
//   mode=shared   50 producers + 50 consumers, all on ONE shared key
//   mode=parallel 50 producer/consumer pairs, each on its OWN key
//
// If parallel >> shared, the server parallelizes fine and the shared-key
// phase is inherently serialized by the single entry lock (Amdahl with a
// ~100% serial fraction); nothing except less lock hold time can help.
//
// Usage: go run ./queueprobe [-mode single|shared|parallel] [-ops 1000000]
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

func skipReply(r *bufio.Reader) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return
	}
	switch line[0] {
	case '$':
		n := 0
		fmt.Sscanf(string(line[1:]), "%d", &n)
		if n > 0 {
			io.CopyN(io.Discard, r, int64(n+2))
		}
	case '*':
		n := 0
		fmt.Sscanf(string(line[1:]), "%d", &n)
		for i := 0; i < n; i++ {
			skipReply(r)
		}
	}
}

func lpushCmd(key string) []byte {
	return []byte("*3\r\n$5\r\nLPUSH\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$1\r\nv\r\n")
}

func rpopCmd(key string) []byte {
	return []byte("*2\r\n$4\r\nRPOP\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n")
}

// One LPUSH carrying k values: the wire-level equivalent of what
// dispatch-level coalescing would execute — a single lock acquisition for
// k pushes.
func lpushMultiCmd(key string, k int) []byte {
	b := []byte("*" + strconv.Itoa(2+k) + "\r\n$5\r\nLPUSH\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n")
	for i := 0; i < k; i++ {
		b = append(b, "$1\r\nv\r\n"...)
	}
	return b
}

// RPOP with a count: pops up to n elements under one lock acquisition.
func rpopCountCmd(key string, n int) []byte {
	ns := strconv.Itoa(n)
	return []byte("*3\r\n$4\r\nRPOP\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n$" + strconv.Itoa(len(ns)) + "\r\n" + ns + "\r\n")
}

// Consume one RPOP-with-count reply (an array of bulks, possibly empty or
// nil) and return how many values it carried.
func popReplyCount(r *bufio.Reader) int {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return 0
	}
	switch line[0] {
	case '*':
		n := 0
		fmt.Sscanf(string(line[1:]), "%d", &n)
		if n <= 0 {
			return 0
		}
		for i := 0; i < n; i++ {
			skipReply(r)
		}
		return n
	case '$':
		n := 0
		fmt.Sscanf(string(line[1:]), "%d", &n)
		if n > 0 {
			io.CopyN(io.Discard, r, int64(n+2))
			return 1
		}
	}
	return 0
}

func dial(addr string) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	return c
}

// pipeline `total` copies of `cmd`, reading replies in batches of 100.
func runWorker(c net.Conn, cmd []byte, total int) {
	r := bufio.NewReaderSize(c, 64<<10)
	sent := 0
	for sent < total {
		batch := 100
		if total-sent < batch {
			batch = total - sent
		}
		buf := make([]byte, 0, batch*len(cmd))
		for i := 0; i < batch; i++ {
			buf = append(buf, cmd...)
		}
		if _, err := c.Write(buf); err != nil {
			return
		}
		for i := 0; i < batch; i++ {
			skipReply(r)
		}
		sent += batch
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "server address")
	mode := flag.String("mode", "single", "single | shared | parallel")
	ops := flag.Int("ops", 1_000_000, "total commands")
	flag.Parse()

	start := time.Now()
	var wg sync.WaitGroup

	switch *mode {
	case "single":
		// One connection: LPUSH half, RPOP half, on one key. The queue
		// drains to empty, matching a single-threaded producer-consumer.
		c := dial(*addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := bufio.NewReaderSize(c, 64<<10)
			half := *ops / 2
			cmds := [][]byte{lpushCmd("q"), rpopCmd("q")}
			totals := []int{half, half}
			for i, cmd := range cmds {
				sent := 0
				for sent < totals[i] {
					batch := 100
					if totals[i]-sent < batch {
						batch = totals[i] - sent
					}
					buf := make([]byte, 0, batch*len(cmd))
					for j := 0; j < batch; j++ {
						buf = append(buf, cmd...)
					}
					if _, err := c.Write(buf); err != nil {
						return
					}
					for j := 0; j < batch; j++ {
						skipReply(r)
					}
					sent += batch
				}
			}
		}()
	case "shared":
		// 50 producers + 50 consumers on ONE key — the bench pattern.
		const half = 50
		perSide := *ops / (half * 2)
		for i := 0; i < half; i++ {
			c := dial(*addr)
			wg.Add(1)
			go func(c net.Conn) { defer wg.Done(); runWorker(c, lpushCmd("wq"), perSide) }(c)
		}
		for i := 0; i < half; i++ {
			c := dial(*addr)
			wg.Add(1)
			go func(c net.Conn) { defer wg.Done(); runWorker(c, rpopCmd("wq"), perSide) }(c)
		}
	case "parallel":
		// 50 producer/consumer pairs, each pair on its OWN key. Same total
		// command count, but the work is spread across independent entries.
		const pairs = 50
		perSide := *ops / (pairs * 2)
		for i := 0; i < pairs; i++ {
			key := fmt.Sprintf("pq%d", i)
			pc := dial(*addr)
			cc := dial(*addr)
			wg.Add(2)
			go func(c net.Conn) { defer wg.Done(); runWorker(c, lpushCmd(key), perSide) }(pc)
			go func(c net.Conn) { defer wg.Done(); runWorker(c, rpopCmd(key), perSide) }(cc)
		}
	case "variadic":
		// Same shape and value count as "shared" (50 producers + 50
		// consumers on ONE key), but each side batches 100 values per
		// command — one lock acquisition per 100 values. This is the
		// measured ceiling of dispatch-level coalescing: the wire says
		// "batch", the server does what a coalescer would do.
		const half = 50
		pushVals := *ops / 2
		popVals := *ops - pushVals
		perPush := pushVals / half
		perPop := popVals / half

		for i := 0; i < half; i++ {
			c := dial(*addr)
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				r := bufio.NewReaderSize(c, 128<<10)
				remaining := perPush
				for remaining > 0 {
					// Pipeline 10 variadic commands (up to 1000 values)
					// per round trip.
					buf := make([]byte, 0, 64*1024)
					cmds := 0
					for cmds < 10 && remaining > 0 {
						k := 100
						if remaining < k {
							k = remaining
						}
						buf = append(buf, lpushMultiCmd("wq", k)...)
						remaining -= k
						cmds++
					}
					if _, err := c.Write(buf); err != nil {
						return
					}
					for i := 0; i < cmds; i++ {
						skipReply(r)
					}
				}
			}(c)
		}
		for i := 0; i < half; i++ {
			c := dial(*addr)
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				r := bufio.NewReaderSize(c, 128<<10)
				need := perPop
				for need > 0 {
					// The queue may run dry mid-batch; keep issuing
					// count-pops until the quota of values is met.
					k := 100
					if need < k {
						k = need
					}
					if _, err := c.Write(rpopCountCmd("wq", k)); err != nil {
						return
					}
					need -= popReplyCount(r)
				}
			}(c)
		}
	default:
		panic("unknown mode")
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("mode=%-8s ops=%d  elapsed=%dms  %.2fM ops/sec\n",
		*mode, *ops, elapsed.Milliseconds(),
		float64(*ops)/elapsed.Seconds()/1e6)
}
