// Subscriber-scaling probe for the pub/sub publish path. Not part of the
// standard benchmark: exists to answer "does publish cost scale with
// subscriber count?", which separates fan-out cost from fixed per-publish
// cost.
//
// Usage: go run ./probe [-subs 10] [-pubs 10] [-msgs 20000]
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

func readN(r *bufio.Reader, n int) {
	for i := 0; i < n; i++ {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		switch line[0] {
		case ':', '+', '-':
		case '$':
			ln := 0
			fmt.Sscanf(string(line[1:]), "%d", &ln)
			if ln >= 0 {
				if _, err := io.ReadFull(r, make([]byte, ln+2)); err != nil {
					return
				}
			}
		case '*':
			cnt := 0
			fmt.Sscanf(string(line[1:]), "%d", &cnt)
			for j := 0; j < cnt; j++ {
				readN(r, 1)
			}
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "server address")
	subs := flag.Int("subs", 50, "subscribers")
	pubs := flag.Int("pubs", 10, "publishers")
	msgs := flag.Int("msgs", 20000, "messages per publisher")
	pipe := flag.Int("pipe", 200, "pipeline depth")
	flag.Parse()

	subConns := make([]net.Conn, *subs)
	for i := range subConns {
		c, err := net.Dial("tcp", *addr)
		if err != nil {
			fmt.Println("dial:", err)
			os.Exit(1)
		}
		c.(*net.TCPConn).SetNoDelay(true)
		subConns[i] = c
	}
	pubConns := make([]net.Conn, *pubs)
	for i := range pubConns {
		c, err := net.Dial("tcp", *addr)
		if err != nil {
			fmt.Println("dial:", err)
			os.Exit(1)
		}
		c.(*net.TCPConn).SetNoDelay(true)
		pubConns[i] = c
	}

	start := time.Now()
	var subWg, pubWg sync.WaitGroup
	deadline := time.Now().Add(30 * time.Second)

	// One delivery frame, fixed size: *3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$3\r\nmsg\r\n
	frameBytes := int64(len("*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$3\r\nmsg\r\n"))
	totalMsgs := int64(*pubs) * int64(*msgs)
	// Subscribe synchronously: publishers starting concurrently would race
	// their first batch past the SUBSCRIBE, and the subscriber would then
	// wait forever for messages that were never routed to it.
	for _, c := range subConns {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		r := bufio.NewReaderSize(c, 512)
		c.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nchan\r\n"))
		readN(r, 1) // subscription confirmation
		// Drain anything the confirmation read buffered so the byte counter
		// below starts aligned.
		if n := r.Buffered(); n > 0 {
			io.CopyN(io.Discard, r, int64(n))
		}
		c.SetReadDeadline(deadline)
	}

	for _, c := range subConns {
		subWg.Add(1)
		go func(c net.Conn) {
			defer subWg.Done()
			// Fixed-size frames let the reader count bytes instead of
			// parsing, so the client cannot become the bottleneck.
			io.CopyN(io.Discard, c, totalMsgs*frameBytes)
			c.Close()
		}(c)
	}

	for _, c := range pubConns {
		pubWg.Add(1)
		go func(c net.Conn) {
			defer pubWg.Done()
			c.SetReadDeadline(deadline)
			r := bufio.NewReaderSize(c, 64<<10)
			sent := 0
			cmd := []byte("*3\r\n$7\r\nPUBLISH\r\n$4\r\nchan\r\n$3\r\nmsg\r\n")
			for sent < *msgs {
				batch := *pipe
				if *msgs-sent < batch {
					batch = *msgs - sent
				}
				buf := make([]byte, 0, batch*len(cmd))
				for i := 0; i < batch; i++ {
					buf = append(buf, cmd...)
				}
				if _, err := c.Write(buf); err != nil {
					fmt.Println("pub write err:", err)
					return
				}
				readN(r, batch)
				sent += batch
			}
			c.Close()
		}(c)
	}

	pubWg.Wait()
	pubDone := time.Since(start)
	subWg.Wait()
	e2e := time.Since(start)

	total := int64(*pubs) * int64(*msgs)
	deliveries := int64(*subs) * total
	fmt.Printf("subs=%d pubs=%d msgs=%d: publish=%dms (%.0fk/s)  e2e=%dms  delivery=%.1fM/s\n",
		*subs, *pubs, *msgs,
		pubDone.Milliseconds(), float64(total)/pubDone.Seconds()/1000,
		e2e.Milliseconds(),
		float64(deliveries)/e2e.Seconds()/1e6)
}
