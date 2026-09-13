package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func setVar[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

const (
	cmdOther = iota
	cmdSet
	cmdGet
	cmdIncr
	cmdHset
	cmdHget
	cmdLpush
	cmdRpop
	cmdSadd
	cmdZadd
	cmdExpire
	cmdJsonSet
	cmdJsonGet
	cmdPublish
	cmdSubscribe
	cmdFlush
	cmdPing
	cmdCount
)

var cmdNames = [cmdCount]string{
	cmdSet:       "SET",
	cmdGet:       "GET",
	cmdIncr:      "INCR",
	cmdHset:      "HSET",
	cmdHget:      "HGET",
	cmdLpush:     "LPUSH",
	cmdRpop:      "RPOP",
	cmdSadd:      "SADD",
	cmdZadd:      "ZADD",
	cmdExpire:    "EXPIRE",
	cmdJsonSet:   "JSON.SET",
	cmdJsonGet:   "JSON.GET",
	cmdPublish:   "PUBLISH",
	cmdSubscribe: "SUBSCRIBE",
	cmdFlush:     "FLUSHALL",
	cmdPing:      "PING",
}

var (
	errBadResp  = errors.New("bad RESP")
	errLongLine = errors.New("RESP line too long")
)

func cmdID(name []byte) int {
	switch string(name) {
	case "SET":
		return cmdSet
	case "GET":
		return cmdGet
	case "INCR":
		return cmdIncr
	case "HSET":
		return cmdHset
	case "HGET":
		return cmdHget
	case "LPUSH":
		return cmdLpush
	case "RPOP":
		return cmdRpop
	case "SADD":
		return cmdSadd
	case "ZADD":
		return cmdZadd
	case "EXPIRE":
		return cmdExpire
	case "JSON.SET":
		return cmdJsonSet
	case "JSON.GET":
		return cmdJsonGet
	case "PUBLISH":
		return cmdPublish
	case "SUBSCRIBE":
		return cmdSubscribe
	case "FLUSHALL":
		return cmdFlush
	case "PING":
		return cmdPing
	}
	return cmdOther
}

type fakeServer struct {
	ln        net.Listener
	wg        sync.WaitGroup
	mu        sync.Mutex
	subs      map[string][]*subConn
	conns     []*connCounts
	other     map[string]int64
	delivered atomic.Int64
}

type connCounts struct {
	n [cmdCount]atomic.Int64
}

type subConn struct {
	conn net.Conn
	wmu  sync.Mutex
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{
		ln:    ln,
		subs:  make(map[string][]*subConn),
		other: make(map[string]int64),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Go(func() {
				s.serve(conn)
			})
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReaderSize(conn, 64<<10)
	w := bufio.NewWriterSize(conn, 64<<10)
	cc := &connCounts{}
	s.mu.Lock()
	s.conns = append(s.conns, cc)
	s.mu.Unlock()
	for {
		id, name, a2, a3, err := readCmd(r)
		if err != nil {
			return
		}
		s.reply(w, cc, conn, id, a2, a3)
		s.noteOther(id, name)
		for r.Buffered() > 0 {
			id, name, a2, a3, err = readCmd(r)
			if err != nil {
				w.Flush()
				return
			}
			s.reply(w, cc, conn, id, a2, a3)
			s.noteOther(id, name)
		}
		if w.Flush() != nil {
			return
		}
	}
}

func (s *fakeServer) noteOther(id int, name []byte) {
	if id == cmdOther {
		s.mu.Lock()
		s.other[string(name)]++
		s.mu.Unlock()
	}
}

func (s *fakeServer) reply(w *bufio.Writer, cc *connCounts, conn net.Conn, id int, ch, payload []byte) {
	cc.n[id].Add(1)
	switch id {
	case cmdSubscribe:
		s.addSubscriber(conn, string(ch))
	case cmdPublish:
		n := s.fanOut(ch, payload)
		var b [8]byte
		w.WriteByte(':')
		w.Write(strconv.AppendInt(b[:0], int64(n), 10))
		w.WriteString("\r\n")
	case cmdPing:
		w.WriteString("+PONG\r\n")
	case cmdSet, cmdFlush, cmdJsonSet:
		w.WriteString("+OK\r\n")
	case cmdGet, cmdHget, cmdRpop, cmdJsonGet:
		w.WriteString("$5\r\nvalue\r\n")
	default:
		w.WriteString(":1\r\n")
	}
}

func (s *fakeServer) addSubscriber(conn net.Conn, channel string) {
	sc := &subConn{conn: conn}
	s.mu.Lock()
	s.subs[channel] = append(s.subs[channel], sc)
	s.mu.Unlock()
	frame := "*3\r\n$9\r\nsubscribe\r\n$" + strconv.Itoa(len(channel)) + "\r\n" + channel + "\r\n:1\r\n"
	sc.wmu.Lock()
	writeAll(sc.conn, []byte(frame))
	sc.wmu.Unlock()
}

func (s *fakeServer) fanOut(channel, payload []byte) int {
	frame := appendMessageFrame(nil, channel, payload)
	s.mu.Lock()
	targets := append([]*subConn(nil), s.subs[string(channel)]...)
	s.mu.Unlock()
	for _, sc := range targets {
		sc.wmu.Lock()
		writeAll(sc.conn, frame)
		sc.wmu.Unlock()
		s.delivered.Add(1)
	}
	return len(targets)
}

func appendMessageFrame(dst []byte, channel, payload []byte) []byte {
	dst = append(dst, "*3\r\n$7\r\nmessage\r\n$"...)
	dst = strconv.AppendInt(dst, int64(len(channel)), 10)
	dst = append(dst, "\r\n"...)
	dst = append(dst, channel...)
	dst = append(dst, "\r\n$"...)
	dst = strconv.AppendInt(dst, int64(len(payload)), 10)
	dst = append(dst, "\r\n"...)
	dst = append(dst, payload...)
	return append(dst, "\r\n"...)
}

func writeAll(w io.Writer, p []byte) {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			panic(err)
		}
		p = p[n:]
	}
}

func (s *fakeServer) count(cmd string) int64 {
	if cmd == "MESSAGE" {
		return s.delivered.Load()
	}
	for id, name := range cmdNames {
		if name == cmd {
			s.mu.Lock()
			defer s.mu.Unlock()
			var n int64
			for _, cc := range s.conns {
				n += cc.n[id].Load()
			}
			return n
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.other[cmd]
}

func (s *fakeServer) snapshot() map[string]int64 {
	t := make(map[string]int64, len(cmdNames)+2)
	s.mu.Lock()
	for _, cc := range s.conns {
		for id, name := range cmdNames {
			if v := cc.n[id].Load(); v != 0 {
				t[name] += v
			}
		}
	}
	for k, v := range s.other {
		t[k] += v
	}
	s.mu.Unlock()
	t["MESSAGE"] = s.delivered.Load()
	return t
}

func (s *fakeServer) delta(before map[string]int64) map[string]int64 {
	now := s.snapshot()
	out := make(map[string]int64, len(now))
	for k, v := range now {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

func readCmd(r *bufio.Reader) (id int, name, a2, a3 []byte, err error) {
	line, err := readLine(r)
	if err != nil {
		return
	}
	if len(line) < 2 || line[0] != '*' {
		return 0, nil, nil, nil, errBadResp
	}
	n := parseLen(line[1:])
	if n < 1 {
		return 0, nil, nil, nil, errBadResp
	}
	if name, err = readBulkArg(r); err != nil {
		return
	}
	id = cmdID(name)
	rest := n - 1
	if id == cmdSubscribe && rest >= 1 {
		if a2, err = readBulkArg(r); err != nil {
			return
		}
		a2 = append([]byte(nil), a2...)
		rest--
	} else if id == cmdPublish && rest >= 2 {
		if a2, err = readBulkArg(r); err != nil {
			return
		}
		if a3, err = readBulkArg(r); err != nil {
			return
		}
		a2 = append([]byte(nil), a2...)
		a3 = append([]byte(nil), a3...)
		rest -= 2
	}
	for ; rest > 0; rest-- {
		if err = skipBulk(r); err != nil {
			return
		}
	}
	return
}

func readBulkArg(r *bufio.Reader) ([]byte, error) {
	hdr, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(hdr) < 1 || hdr[0] != '$' {
		return nil, errBadResp
	}
	n := parseLen(hdr[1:])
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == n+2 && line[n] == '\r' {
		return line[:n], nil
	}
	return nil, errBadResp
}

func skipBulk(r *bufio.Reader) error {
	hdr, err := readLine(r)
	if err != nil {
		return err
	}
	if len(hdr) < 1 || hdr[0] != '$' {
		return errBadResp
	}
	total := parseLen(hdr[1:]) + 2
	for total > 0 {
		d, err := r.Discard(total)
		if err != nil {
			return err
		}
		total -= d
	}
	return nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, errLongLine
	}
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errBadResp
	}
	return line[:len(line)-2], nil
}

func parseLen(b []byte) int {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func TestKVRequestCounts(t *testing.T) {
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &CLIENTS, 4)
	setVar(t, &OPS_CLIENT, 250)

	runKV()

	total := int64(CLIENTS * OPS_CLIENT)
	if got := srv.count("SET"); got != 2*total {
		t.Errorf("SET = %d, want %d (SET runs in two phases)", got, 2*total)
	}
	if got := srv.count("GET"); got != total {
		t.Errorf("GET = %d, want %d", got, total)
	}
	if got := srv.count("FLUSHALL"); got != 1 {
		t.Errorf("FLUSHALL = %d, want 1", got)
	}
}

func TestClusterKVRequestCounts(t *testing.T) {
	const nodes = 3
	srvs := make([]*fakeServer, nodes)
	cluster := make([]string, nodes)
	for i := range srvs {
		srvs[i] = startFakeServer(t)
		cluster[i] = srvs[i].addr()
	}
	setVar(t, &addrs, cluster)
	setVar(t, &CLIENTS, 6)
	setVar(t, &OPS_CLIENT, 250)

	runKV()

	total := int64(CLIENTS * OPS_CLIENT)
	var setAll, getAll int64
	for i, s := range srvs {
		if s.count("SET") == 0 || s.count("GET") == 0 {
			t.Errorf("node %d received no traffic (SET=%d GET=%d)",
				i, s.count("SET"), s.count("GET"))
		}
		setAll += s.count("SET")
		getAll += s.count("GET")
	}
	if setAll != 2*total {
		t.Errorf("cluster SET = %d, want %d", setAll, 2*total)
	}
	if getAll != total {
		t.Errorf("cluster GET = %d, want %d", getAll, total)
	}
}

func TestWarmupRequestCounts(t *testing.T) {
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &CLIENTS, 4)

	warmup()

	total := int64(CLIENTS) * 4 * 50
	if got := srv.count("SET"); got != total {
		t.Errorf("SET = %d, want %d", got, total)
	}
	if got := srv.count("GET"); got != total {
		t.Errorf("GET = %d, want %d", got, total)
	}
	if got := srv.count("FLUSHALL"); got != 1 {
		t.Errorf("FLUSHALL = %d, want 1", got)
	}
}

func TestMixRequestCounts(t *testing.T) {
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &CLIENTS, 4)
	setVar(t, &MIX_OPS_CLIENT, 200)
	setVar(t, &MIX_HOTKEY_OPS, 500)
	setVar(t, &MIX_QUEUE_OPS, 200)

	half := int64(CLIENTS / 2)
	ops := int64(CLIENTS * MIX_OPS_CLIENT)
	hotOps := int64(CLIENTS) * int64(MIX_HOTKEY_OPS)

	phase := func(name string, run func(), want map[string]int64) {
		t.Run(name, func(t *testing.T) {
			before := srv.snapshot()
			run()
			got := srv.delta(before)
			for cmd, n := range want {
				if got[cmd] != n {
					t.Errorf("%s = %d, want %d", cmd, got[cmd], n)
				}
			}
			for cmd, n := range got {
				if _, ok := want[cmd]; !ok {
					t.Errorf("unexpected command %s = %d", cmd, n)
				}
			}
		})
	}

	phase("mixed", runBenchMixed, map[string]int64{"SET": ops / 2, "GET": ops / 2})
	phase("incr", runBenchIncr, map[string]int64{"INCR": ops})
	phase("hash", runBenchHash, map[string]int64{"HSET": ops / 2, "HGET": ops / 2})
	phase("list", runBenchList, map[string]int64{"LPUSH": ops / 2, "RPOP": ops / 2})
	phase("set", runBenchSet, map[string]int64{"SADD": ops})
	phase("zset", runBenchZSet, map[string]int64{"ZADD": ops})
	phase("expire", runBenchExpire, map[string]int64{"SET": ops, "EXPIRE": ops})
	phase("hotkey", runBenchHotKey, map[string]int64{"SET": hotOps / 2, "GET": hotOps / 2})
	phase("queue", runBenchQueue, map[string]int64{
		"LPUSH": half * int64(MIX_QUEUE_OPS),
		"RPOP":  half * int64(MIX_QUEUE_OPS),
	})
	phase("json", runBenchJson, map[string]int64{
		"JSON.SET": ops/2 + 1,
		"JSON.GET": ops / 2,
	})
}

func TestClusterMixRequestCounts(t *testing.T) {
	const nodes = 3
	srvs := make([]*fakeServer, nodes)
	cluster := make([]string, nodes)
	for i := range srvs {
		srvs[i] = startFakeServer(t)
		cluster[i] = srvs[i].addr()
	}
	setVar(t, &addrs, cluster)
	setVar(t, &CLIENTS, 6)
	setVar(t, &MIX_OPS_CLIENT, 200)

	snapshotAll := func() []map[string]int64 {
		snaps := make([]map[string]int64, len(srvs))
		for i, s := range srvs {
			snaps[i] = s.snapshot()
		}
		return snaps
	}
	deltaAll := func(before []map[string]int64) map[string]int64 {
		out := make(map[string]int64)
		for i, s := range srvs {
			for cmd, n := range s.delta(before[i]) {
				out[cmd] += n
			}
		}
		return out
	}

	t.Run("mixed", func(t *testing.T) {
		before := snapshotAll()
		runBenchMixed()
		got := deltaAll(before)
		ops := int64(CLIENTS * MIX_OPS_CLIENT)
		if got["SET"] != ops/2 {
			t.Errorf("SET = %d, want %d", got["SET"], ops/2)
		}
		if got["GET"] != ops/2 {
			t.Errorf("GET = %d, want %d", got["GET"], ops/2)
		}
	})

	t.Run("expire", func(t *testing.T) {
		before := snapshotAll()
		runBenchExpire()
		got := deltaAll(before)
		ops := int64(CLIENTS * MIX_OPS_CLIENT)
		if got["SET"] != ops {
			t.Errorf("SET = %d, want %d", got["SET"], ops)
		}
		if got["EXPIRE"] != ops {
			t.Errorf("EXPIRE = %d, want %d", got["EXPIRE"], ops)
		}
	})
}

func TestFullMixRequestCounts(t *testing.T) {
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &CLIENTS, 4)
	setVar(t, &MIX_OPS_CLIENT, 200)
	setVar(t, &MIX_HOTKEY_OPS, 500)
	setVar(t, &MIX_QUEUE_OPS, 200)

	runMix()

	ops := int64(CLIENTS * MIX_OPS_CLIENT)
	hotOps := int64(CLIENTS) * int64(MIX_HOTKEY_OPS)
	half := int64(CLIENTS / 2)

	want := map[string]int64{
		"SET":      ops/2 + ops + hotOps/2,
		"GET":      ops/2 + hotOps/2,
		"INCR":     ops,
		"HSET":     ops / 2,
		"HGET":     ops / 2,
		"LPUSH":    ops/2 + half*int64(MIX_QUEUE_OPS),
		"RPOP":     ops/2 + half*int64(MIX_QUEUE_OPS),
		"SADD":     ops,
		"ZADD":     ops,
		"EXPIRE":   ops,
		"JSON.SET": ops/2 + 1,
		"JSON.GET": ops / 2,
		"FLUSHALL": 10,
		"PING":     10,
	}
	var total int64
	for cmd, n := range want {
		if got := srv.count(cmd); got != n {
			t.Errorf("%s = %d, want %d", cmd, got, n)
		}
		total += n
	}
	var seen int64
	for _, n := range srv.snapshot() {
		seen += n
	}
	if seen != total {
		t.Errorf("total requests on the wire = %d, want %d (unexpected commands)", seen, total)
	}
}

func TestPubSubRequestCounts(t *testing.T) {
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &PUB_SUBSCRIBERS, 3)
	setVar(t, &PUB_PUBLISHERS, 2)
	setVar(t, &PUB_MSGS_EACH, 400)

	runPubSub(srv.addr())

	publishes := int64(PUB_PUBLISHERS * PUB_MSGS_EACH)
	if got := srv.count("SUBSCRIBE"); got != int64(PUB_SUBSCRIBERS) {
		t.Errorf("SUBSCRIBE = %d, want %d", got, PUB_SUBSCRIBERS)
	}
	if got := srv.count("PUBLISH"); got != publishes {
		t.Errorf("PUBLISH = %d, want %d", got, publishes)
	}
	if got := srv.count("MESSAGE"); got != publishes*int64(PUB_SUBSCRIBERS) {
		t.Errorf("message frames delivered = %d, want %d",
			got, publishes*int64(PUB_SUBSCRIBERS))
	}
}
