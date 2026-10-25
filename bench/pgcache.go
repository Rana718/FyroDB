package main

// pgcache.go — PostgreSQL wire-protocol benchmark for the fyro-dbcache proxy.
//
// Benchmarks two patterns against a running proxy (-pgport flag):
//   1. Simple query  — Q message, one query per round-trip
//   2. Extended protocol — Parse (once) + Bind/Execute/Sync loop
//
// Usage (proxy must already be running):
//   cd bench && go run . -m pgcache -pgport 15433

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// pgDial opens a TCP connection to the proxy, performs the PostgreSQL startup
// handshake (protocol v3, user=postgres, database=test), and drains until
// ReadyForQuery ('Z'). Returns a buffered reader over the connection.
func pgDial(addr string) (net.Conn, *bufio.Reader, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	tc := conn.(*net.TCPConn)
	tc.SetNoDelay(true)
	tc.SetWriteBuffer(2 << 20)
	tc.SetReadBuffer(2 << 20)
	raw, _ := tc.SyscallConn()
	_ = raw.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)
	})

	// PostgreSQL startup: 4-byte length + 4-byte proto version + params + \0\0
	const protoV3 = 196608 // 3 << 16
	params := []byte("user\x00postgres\x00database\x00test\x00\x00")
	totalLen := 4 + 4 + len(params)
	msg := make([]byte, 0, totalLen)
	msg = binary.BigEndian.AppendUint32(msg, uint32(totalLen))
	msg = binary.BigEndian.AppendUint32(msg, protoV3)
	msg = append(msg, params...)
	if _, err := conn.Write(msg); err != nil {
		conn.Close()
		return nil, nil, err
	}

	r := bufio.NewReaderSize(conn, 256<<10)
	if err := pgDrainReady(r); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("startup: %w", err)
	}
	return conn, r, nil
}

// pgDrainReady reads PostgreSQL backend messages until ReadyForQuery ('Z').
// Returns an error if the connection closes or an ErrorResponse ('E') arrives.
func pgDrainReady(r *bufio.Reader) error {
	var lenBuf [4]byte
	for {
		tag, err := r.ReadByte()
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return err
		}
		bodyLen := int(binary.BigEndian.Uint32(lenBuf[:])) - 4
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return err
		}
		if tag == 'Z' {
			return nil
		}
		if tag == 'E' {
			return fmt.Errorf("postgres error: %s", body)
		}
	}
}

// pgSimpleFrame builds a PG simple-query ('Q') frame for sql.
func pgSimpleFrame(sql string) []byte {
	b := make([]byte, 0, len(sql)+6)
	b = append(b, 'Q')
	msgLen := uint32(len(sql) + 5) // 4 (len field) + sql bytes + null terminator
	b = binary.BigEndian.AppendUint32(b, msgLen)
	b = append(b, []byte(sql)...)
	b = append(b, 0)
	return b
}

// pgExtFrames builds the per-loop Bind+Execute+Sync triple for the extended protocol.
// The Parse frame is separate (sent once per connection before the loop).
func pgParseFrame(stmtName, sql string) []byte {
	body := make([]byte, 0, len(stmtName)+len(sql)+10)
	body = append(body, []byte(stmtName)...)
	body = append(body, 0)
	body = append(body, []byte(sql)...)
	body = append(body, 0)
	body = binary.BigEndian.AppendUint16(body, 0) // no param type OIDs
	return pgFrame('P', body)
}

func pgBindFrame(portal, stmt string, paramVal []byte) []byte {
	body := make([]byte, 0, 64)
	body = append(body, []byte(portal)...)
	body = append(body, 0)
	body = append(body, []byte(stmt)...)
	body = append(body, 0)
	body = binary.BigEndian.AppendUint16(body, 0) // no format codes
	body = binary.BigEndian.AppendUint16(body, 1) // 1 parameter
	body = binary.BigEndian.AppendUint32(body, uint32(len(paramVal)))
	body = append(body, paramVal...)
	body = binary.BigEndian.AppendUint16(body, 0) // default result format
	return pgFrame('B', body)
}

func pgExecuteFrame(portal string) []byte {
	body := make([]byte, 0, len(portal)+5)
	body = append(body, []byte(portal)...)
	body = append(body, 0)
	body = binary.BigEndian.AppendUint32(body, 0) // max rows = 0 (unlimited)
	return pgFrame('E', body)
}

func pgFrame(tag byte, body []byte) []byte {
	out := make([]byte, 0, 1+4+len(body))
	out = append(out, tag)
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)+4))
	out = append(out, body...)
	return out
}

// ── Simple query benchmark ─────────────────────────────────────────────────

func pgBenchSimple(addr string, clients, opsEach int) {
	sql := "SELECT id, username, email FROM users WHERE id = 42;"
	frame := pgSimpleFrame(sql)
	totalOps := int64(clients * opsEach)

	var done atomic.Int64
	var wg sync.WaitGroup
	wg.Add(clients)

	start := time.Now()
	for range clients {
		go func() {
			defer wg.Done()
			conn, r, err := pgDial(addr)
			if err != nil {
				fmt.Printf("pgcache simple: dial error: %v\n", err)
				return
			}
			defer conn.Close()
			for range opsEach {
				if _, err := conn.Write(frame); err != nil {
					return
				}
				if err := pgDrainReady(r); err != nil {
					return
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	got := done.Load()
	fmt.Printf("── PG Simple Query  (%s)\n", addr)
	fmt.Printf("   clients: %d  ops/client: %d  total: %d\n", clients, opsEach, totalOps)
	fmt.Printf("   ops:     %d\n", got)
	fmt.Printf("   elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("   ops/sec: %s\n\n", fmtRate(rate(got, elapsed)))
}

// ── Extended protocol benchmark ────────────────────────────────────────────

func pgBenchExtended(addr string, clients, opsEach int) {
	sql := "SELECT id, username, email FROM users WHERE id = $1;"
	parseFrame := pgParseFrame("s1", sql)
	bindFrame := pgBindFrame("p1", "s1", []byte("42"))
	execFrame := pgExecuteFrame("p1")
	syncFrame := pgFrame('S', nil)
	// Sync body is empty; pgFrame adds the 4-byte length.
	// Per wire protocol: 'S' + int32(4) — correct.
	_ = syncFrame

	// Build Sync correctly: tag 'S' + length 4 (no body).
	syncMsg := []byte{b_S, 0, 0, 0, 4}

	totalOps := int64(clients * opsEach)
	var done atomic.Int64
	var wg sync.WaitGroup
	wg.Add(clients)

	start := time.Now()
	for range clients {
		go func() {
			defer wg.Done()
			conn, r, err := pgDial(addr)
			if err != nil {
				fmt.Printf("pgcache extended: dial error: %v\n", err)
				return
			}
			defer conn.Close()

			// Send Parse once (no Sync); proxy buffers it.
			if _, err := conn.Write(parseFrame); err != nil {
				return
			}

			// Loop: Bind + Execute + Sync; first Sync also flushes the Parse.
			loop := make([]byte, 0, len(bindFrame)+len(execFrame)+5)
			loop = append(loop, bindFrame...)
			loop = append(loop, execFrame...)
			loop = append(loop, syncMsg...)

			for range opsEach {
				if _, err := conn.Write(loop); err != nil {
					return
				}
				if err := pgDrainReady(r); err != nil {
					return
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	got := done.Load()
	fmt.Printf("── PG Extended Protocol  (%s)\n", addr)
	fmt.Printf("   clients: %d  ops/client: %d  total: %d\n", clients, opsEach, totalOps)
	fmt.Printf("   ops:     %d\n", got)
	fmt.Printf("   elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("   ops/sec: %s\n\n", fmtRate(rate(got, elapsed)))
}

// b_S is 'S' as a byte constant for use in composite literals.
const b_S = byte('S')

// runPGCache runs the full dbcache proxy benchmark suite against pgAddr.
func runPGCache(pgAddr string, clients, opsEach int) {
	fmt.Printf("── dbcache PostgreSQL Proxy Benchmark  (%s) ─────────────────────────\n", pgAddr)
	fmt.Printf("   clients=%d  ops/client=%d  total=%d\n\n",
		clients, opsEach, int64(clients)*int64(opsEach))

	pgBenchSimple(pgAddr, clients, opsEach)
	pgBenchExtended(pgAddr, clients, opsEach)

	fmt.Println("── dbcache Proxy Summary ───────────────────────────────────────────")
	fmt.Println("   (see per-benchmark ops/sec above)")
	fmt.Println()
}
