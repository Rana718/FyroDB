package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// fastNullServer is a pure RESP loopback server with ZERO database, ZERO locks, and ZERO storage.
// It reads RESP commands from the client and immediately replies +OK\r\n for SET or $5\r\nvalue\r\n for GET.
func startFastNullServer(t *testing.T) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	quit := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tc := conn.(*net.TCPConn)
			tc.SetNoDelay(true)
			tc.SetReadBuffer(2 << 20)
			tc.SetWriteBuffer(2 << 20)

			wg.Add(1)
			go func(c *net.TCPConn) {
				defer wg.Done()
				defer c.Close()
				r := bufio.NewReaderSize(c, 256<<10)
				w := bufio.NewWriterSize(c, 256<<10)

				const okReply = "+OK\r\n"
				for {
					// Read array count *N
					line, err := r.ReadSlice('\n')
					if err != nil {
						return
					}
					if len(line) < 3 || line[0] != '*' {
						continue
					}
					numParts := int(line[1] - '0')
					if numParts < 1 || numParts > 9 {
						numParts = 3
					}

					// Skip bulk parts for this command
					for p := 0; p < numParts; p++ {
						// Read $len
						hdr, err := r.ReadSlice('\n')
						if err != nil {
							return
						}
						if len(hdr) > 1 && hdr[0] == '$' {
							blen := 0
							for k := 1; k < len(hdr)-2; k++ {
								blen = blen*10 + int(hdr[k]-'0')
							}
							// Discard bulk payload + CRLF
							discardN(r, blen+2)
						}
					}

					// Send reply
					w.WriteString(okReply)

					// If more commands are already buffered, process them all before flushing to socket
					for r.Buffered() > 0 {
						line, err := r.ReadSlice('\n')
						if err != nil {
							w.Flush()
							return
						}
						if len(line) < 3 || line[0] != '*' {
							continue
						}
						numParts := int(line[1] - '0')
						if numParts < 1 || numParts > 9 {
							numParts = 3
						}
						for p := 0; p < numParts; p++ {
							hdr, err := r.ReadSlice('\n')
							if err != nil {
								w.Flush()
								return
							}
							if len(hdr) > 1 && hdr[0] == '$' {
								blen := 0
								for k := 1; k < len(hdr)-2; k++ {
									blen = blen*10 + int(hdr[k]-'0')
								}
								discardN(r, blen+2)
							}
						}
						w.WriteString(okReply)
					}

					if err := w.Flush(); err != nil {
						return
					}
				}
			}(tc)
		}
	}()

	cleanup := func() {
		close(quit)
		ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), cleanup
}

func TestNetworkBottleneckAnalysis(t *testing.T) {
	fmt.Println("\n================================================================================")
	fmt.Println("             SCIENTIFIC LAYER-BY-LAYER BOTTLENECK ANALYSIS                      ")
	fmt.Println("================================================================================")

	// Layer 1: In-Memory Request Generation (No Network)
	fmt.Println("── LAYER 1: Raw Client Generation Engine (Memory Only) ──────────────────────────")
	{
		totalOps := int64(10_000_000)
		const numClients = 100
		opsPerClient := int(totalOps / numClients)
		const batch = 5000

		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(id int) {
				defer wg.Done()
				buf := make([]byte, batch*60)
				base := id * opsPerClient
				for sent := 0; sent < opsPerClient; {
					n := min(batch, opsPerClient-sent)
					_ = buildBatchFastSet(base+sent, n, buf)
					sent += n
				}
			}(i)
		}
		wg.Wait()
		el := time.Since(start)
		fmt.Printf("   10M SET formatting (100 goroutines): %v -> %.2fM ops/sec\n", el, float64(totalOps)/el.Seconds()/1e6)

		startGet := time.Now()
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(id int) {
				defer wg.Done()
				buf := make([]byte, batch*60)
				base := id * opsPerClient
				for sent := 0; sent < opsPerClient; {
					n := min(batch, opsPerClient-sent)
					_ = buildBatchFastGet(base+sent, n, buf)
					sent += n
				}
			}(i)
		}
		wg.Wait()
		elGet := time.Since(startGet)
		fmt.Printf("   10M GET formatting (100 goroutines): %v -> %.2fM ops/sec\n", elGet, float64(totalOps)/elGet.Seconds()/1e6)
	}

	// Layer 2: Pure Localhost TCP Network Bandwidth
	fmt.Println("\n── LAYER 2: Linux Loopback (127.0.0.1) Pure TCP Bandwidth ──────────────────────")
	{
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			buf := make([]byte, 256<<10)
			for {
				if _, err := c.Read(buf); err != nil {
					return
				}
			}
		}()

		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		payload := make([]byte, 256<<10)
		start := time.Now()
		var bytesSent int64
		for time.Since(start) < 2*time.Second {
			n, err := c.Write(payload)
			if err != nil {
				break
			}
			bytesSent += int64(n)
		}
		el := time.Since(start)
		gbs := float64(bytesSent) / el.Seconds() / (1024 * 1024 * 1024)
		gbps := float64(bytesSent*8) / el.Seconds() / 1e9
		fmt.Printf("   Loopback TCP stream throughput:       %.2f GB/s (%.2f Gbps)\n", gbs, gbps)
	}

	// Layer 3: Pure TCP Socket + RESP Loopback Ceiling (Null Server, 0 DB overhead)
	fmt.Println("\n── LAYER 3: Pure TCP + RESP Protocol Overhead (Null Server, Zero DB) ───────────")
	{
		addr, cleanup := startFastNullServer(t)
		defer cleanup()

		oldAddrs := addrs
		addrs = []string{addr}
		defer func() { addrs = oldAddrs }()

		for _, clients := range []int{24, 48, 100} {
			conns := preDial(clients)
			totalOps := int64(clients * 100_000)

			start := time.Now()
			var wg sync.WaitGroup
			wg.Add(clients)
			for i := 0; i < clients; i++ {
				go func(id int, conn *net.TCPConn) {
					defer wg.Done()
					base := id * 100_000
					runAsyncPipeline(conn, 100_000,
						func(offset, count int, out []byte) []byte {
							return buildBatchFastSet(base+offset, count, out)
						}, 5)
				}(i, conns[i])
			}
			wg.Wait()
			el := time.Since(start)
			rate := float64(totalOps) / el.Seconds() / 1e6
			fmt.Printf("   clients=%3d: 10M SET over TCP -> %v (%.2fM ops/sec)\n", clients, el, rate)
			closeAll(conns)
		}
	}
	fmt.Println("================================================================================")
}
