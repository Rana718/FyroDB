// Isolates the SET+EXPIRE phase cost: measures SET-only, EXPIRE-only, and
// interleaved SET+EXPIRE pairs over pre-set keys, 100 conns × pipeline 100,
// exactly like the bench phase.
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

func skipN(r *bufio.Reader, n int) {
	for i := 0; i < n; i++ {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		if line[0] == '$' {
			l := 0
			fmt.Sscanf(string(line[1:]), "%d", &l)
			if l > 0 {
				io.CopyN(io.Discard, r, int64(l+2))
			}
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "server")
	mode := flag.String("mode", "pairs", "set | expire | pairs")
	ops := flag.Int("ops", 1_000_000, "total ops")
	flag.Parse()

	const conns = 100
	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < conns; i++ {
		c := dial(*addr)
		wg.Add(1)
		go func(c net.Conn, id int) {
			defer wg.Done()
			r := bufio.NewReaderSize(c, 128<<10)
			buf := make([]byte, 0, 64*1024)
			per := *ops / conns
			for sent := 0; sent < per; {
				batch := 100
				if per-sent < batch {
					batch = per - sent
				}
				buf = buf[:0]
				for j := 0; j < batch; j++ {
					k := []byte("ep:" + strconv.Itoa(id) + ":" + strconv.Itoa(sent+j))
					switch *mode {
					case "set":
						buf = append(buf, "*3\r\n$3\r\nSET\r\n$"...)
						buf = append(buf, strconv.Itoa(len(k))...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n$1\r\nv\r\n"...)
					case "expire":
						buf = append(buf, "*3\r\n$6\r\nEXPIRE\r\n$"...)
						buf = append(buf, strconv.Itoa(len(k))...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n$2\r\n60\r\n"...)
					case "pairs":
						buf = append(buf, "*3\r\n$3\r\nSET\r\n$"...)
						buf = append(buf, strconv.Itoa(len(k))...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n$1\r\nv\r\n"...)
						buf = append(buf, "*3\r\n$6\r\nEXPIRE\r\n$"...)
						buf = append(buf, strconv.Itoa(len(k))...)
						buf = append(buf, "\r\n"...)
						buf = append(buf, k...)
						buf = append(buf, "\r\n$2\r\n60\r\n"...)
					}
				}
				if _, err := c.Write(buf); err != nil {
					return
				}
				replyPer := 1
				if *mode == "pairs" {
					replyPer = 2
				}
				skipN(r, batch*replyPer)
				sent += batch
			}
		}(c, i)
	}
	wg.Wait()
	el := time.Since(start)
	fmt.Printf("mode=%-7s ops=%d  %dms  %.2fM ops/s\n", *mode, *ops, el.Milliseconds(), float64(*ops)/el.Seconds()/1e6)
}
