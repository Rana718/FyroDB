// Protocol-floor probe: PING (no store access) vs SET vs GET at the same
// client shape (100 conns, pipeline 100). The gap between PING and GET is
// the store-read cost; between GET and SET the store-write cost. Whatever
// PING costs is the parse/dispatch/epoll/syscall floor no store-side change
// can remove.
//
// Usage: go run ./floorprobe [-ops 1000000]
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

func dial(addr string) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	return c
}

// Zero-alloc reply skipping, same style as the bench client: allocations
// or fmt.Sscanf here would measure the probe, not the server.
func skipN(r *bufio.Reader, n int) {
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
				continue // $-1 nil
			}
			vlen := 0
			for _, c := range line {
				if c < '0' || c > '9' {
					break
				}
				vlen = vlen*10 + int(c-'0')
			}
			discardN(r, vlen+2)
		case ':', '+':
			if _, err := r.ReadSlice('\n'); err != nil {
				panic(err)
			}
		case '-':
			line, _ := r.ReadSlice('\n')
			panic("server error reply: " + string(line))
		default:
			panic(fmt.Sprintf("unexpected reply byte %q", b))
		}
	}
}

func discardN(r *bufio.Reader, n int) {
	for n > 0 {
		d, err := r.Discard(n)
		n -= d
		if err != nil {
			panic(err)
		}
	}
}

func run(mode, addr string, ops int) {
	const conns = 100
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		c := dial(addr)
		wg.Add(1)
		go func(c net.Conn, id int) {
			defer wg.Done()
			defer c.Close()
			r := bufio.NewReaderSize(c, 128<<10)
			per := ops / conns
			for sent := 0; sent < per; {
				batch := 100
				if per-sent < batch {
					batch = per - sent
				}
				buf := make([]byte, 0, batch*48)
				prefix := "fp:"
				if mode == "set2" || mode == "get2" {
					prefix = "fq:"
				}
				for j := 0; j < batch; j++ {
					k := []byte(prefix + strconv.Itoa(id) + ":" + strconv.Itoa(sent+j))
					kl := strconv.Itoa(len(k))
					switch mode {
					case "ping":
						buf = append(buf, "*1\r\n$4\r\nPING\r\n"...)
					case "set", "set2":
						buf = append(buf, "*3\r\n$3\r\nSET\r\n$"...)
						buf = append(buf, kl...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n$1\r\nv\r\n"...)
					case "get", "get2":
						buf = append(buf, "*2\r\n$3\r\nGET\r\n$"...)
						buf = append(buf, kl...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n"...)
					}
				}
				if _, err := c.Write(buf); err != nil {
					panic(err)
				}
				skipN(r, batch)
				sent += batch
			}
		}(c, i)
	}
	wg.Wait()
	el := time.Since(start)
	fmt.Printf("%-5s ops=%-8d %4dms  %6.2fM ops/s\n", mode, ops, el.Milliseconds(), float64(ops)/el.Seconds()/1e6)
}

// verifyHits checks one GET reply end-to-end: must be a bulk "v", not nil.
func verifyHits(addr string) {
	c := dial(addr)
	defer c.Close()
	k := "fp:0:0"
	c.Write([]byte("*2\r\n$3\r\nGET\r\n$" + strconv.Itoa(len(k)) + "\r\n" + k + "\r\n"))
	r := bufio.NewReader(c)
	line, _ := r.ReadBytes('\n')
	if string(line) != "$1\r\n" {
		panic(fmt.Sprintf("GET %s not a hit: %q", k, line))
	}
	payload := make([]byte, 3)
	io.ReadFull(r, payload)
	if string(payload) != "v\r\n" {
		panic(fmt.Sprintf("bad payload: %q", payload))
	}
	fmt.Printf("hit-verification: GET %s -> $1 v (ok)\n", k)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "server")
	ops := flag.Int("ops", 1_000_000, "total ops per mode")
	flag.Parse()
	run("set", *addr, *ops) // seed keys (also absorbs cold start)
	verifyHits(*addr)
	run("ping", *addr, *ops)
	run("set", *addr, *ops)  // update path (warm)
	run("set2", *addr, *ops) // INSERT path on a warm server (fresh keys)
	run("get", *addr, *ops)
	run("get2", *addr, *ops)
	verifyHits(*addr)
}
