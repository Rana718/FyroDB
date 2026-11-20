package main

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestHashTag(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"plain", ""},
		{"foo{bar}zap", "bar"},
		{"foo{}bar", ""},
		{"foo{}{later}", ""},
		{"foo{bar", ""},
	}
	for _, tt := range tests {
		got := hashTag([]byte(tt.key))
		if string(got) != tt.want {
			t.Fatalf("hashTag(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestKeySlotUsesHashTag(t *testing.T) {
	if got, want := keySlot([]byte("a{shared}1")), keySlot([]byte("b{shared}2")); got != want {
		t.Fatalf("tagged keys mapped to different slots: %d != %d", got, want)
	}
}

func TestClientKeyRoutesToClientNode(t *testing.T) {
	oldAddrs, oldOwners := addrs, slotOwners
	addrs = []string{"node0", "node1", "node2", "node3", "node4", "node5"}
	defer func() { addrs, slotOwners = oldAddrs, oldOwners }()
	slotOwners = make([]int, 16384)
	for i := range slotOwners {
		slotOwners[i] = i % len(addrs)
	}

	var scratch [64]byte
	for id := 0; id < len(addrs); id++ {
		key := clientKey("test:", id, scratch[:])
		if got := nodeForKey(key); got != id {
			t.Fatalf("client %d key routed to node %d", id, got)
		}
	}
}

func TestSkipGetReplies(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("$5\r\nvalue\r\n$-1\r\n:1\r\n"))
	skipGetReplies(r, 3)
	if r.Buffered() != 0 {
		t.Fatalf("reader has %d bytes left", r.Buffered())
	}
}

func TestSkipLinesRejectsRedisError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected Redis error reply to panic")
		}
	}()
	skipLines(bufio.NewReader(strings.NewReader("-ERR unsupported\r\n")), 1)
}

func TestAppendSetBytes(t *testing.T) {
	got := appendSetBytes(nil, []byte("user:42"))
	want := "*3\r\n$3\r\nSET\r\n$7\r\nuser:42\r\n$5\r\nvalue\r\n"
	if string(got) != want {
		t.Errorf("appendSetBytes = %q, want %q", got, want)
	}
}

func TestAppendGetBytes(t *testing.T) {
	got := appendGetBytes(nil, []byte("k"))
	want := "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	if string(got) != want {
		t.Errorf("appendGetBytes = %q, want %q", got, want)
	}
}

func TestAppendLen(t *testing.T) {
	for _, c := range []struct {
		n    int
		want string
	}{
		{0, "0"}, {5, "5"}, {9, "9"}, {10, "10"}, {123, "123"}, {1234567, "1234567"},
	} {
		if got := string(appendLen(nil, c.n)); got != c.want {
			t.Errorf("appendLen(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestKeySlotRedisVectors(t *testing.T) {
	if got := keySlot([]byte("foo")); got != 12182 {
		t.Errorf("keySlot(foo) = %d, want 12182", got)
	}
	if got := keySlot([]byte("123456789")); got != 12739 {
		t.Errorf("keySlot(123456789) = %d, want 12739", got)
	}
}

type loopReader struct {
	b   []byte
	off int
}

func (l *loopReader) Read(p []byte) (int, error) {
	n := copy(p, l.b[l.off:])
	if n == 0 {
		l.off = 0
		n = copy(p, l.b)
	}
	l.off = (l.off + n) % len(l.b)
	return n, nil
}

func BenchmarkSkipGetReplies(b *testing.B) {
	src := &loopReader{b: bytes.Repeat([]byte(fastGetFrame), 8192)}
	r := bufio.NewReaderSize(src, 64<<10)
	for b.Loop() {
		skipGetReplies(r, 512)
	}
}

func TestIndexSlotOwnersUsesAdvertisedRanges(t *testing.T) {
	r := func(lo, hi int64, id string) any { return []any{lo, hi, []any{"localhost", int64(6379), id}} }
	ids := map[string]int{"b": 0, "a": 1}
	owners, err := indexSlotOwners([]any{r(0, 1000, "a"), r(1001, 16383, "b")}, ids)
	if err != nil || owners[1000] != 1 || owners[1001] != 0 {
		t.Fatal(err)
	}
	for _, bad := range [][]any{{r(0, 1000, "a"), r(1000, 16383, "b")}, {r(0, 1000, "a"), r(1002, 16383, "b")}, {r(0, 16383, "unknown")}, {r(0, 16383, "a")}} {
		if _, err := indexSlotOwners(bad, ids); err == nil {
			t.Fatal("accepted invalid slot ownership", bad)
		}
	}
}
