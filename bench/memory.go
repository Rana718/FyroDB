package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Memory-efficiency phase (`-m mem`), after Dragonfly's methodology: every
// scenario starts from an empty-database baseline, loads K keys × M elements,
// and reports logical (used_memory) and resident (used_memory_rss) bytes per
// key and per element. Each container type is measured in both its compact
// and its large encoding, because the transition is where overhead hides.
type memScenario struct {
	name  string
	keys  int
	elems int // elements per key (1 for strings)
	vsize int
	build func(out []byte, key []byte, elem int, val []byte) []byte
	// replies per key: one per command appended by build
	cmdsPerKey int
}

type memResult struct {
	Scenario       string  `json:"scenario"`
	Keys           int     `json:"keys"`
	Elems          int     `json:"elems_per_key"`
	ValueBytes     int     `json:"value_bytes"`
	UsedPerKey     float64 `json:"used_bytes_per_key"`
	RSSPerKey      float64 `json:"rss_bytes_per_key"`
	UsedPerElem    float64 `json:"used_bytes_per_elem"`
	OverheadPerKey float64 `json:"overhead_bytes_per_key"`
}

var memResults []memResult

func ioReadFull(r io.Reader, b []byte) (int, error) { return io.ReadFull(r, b) }

func splitLines(s string) []string {
	return strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
}

func memSample(addr string) (used, rss int64) {
	used, _ = strconv.ParseInt(strings.TrimSpace(infoField(addr, "memory", "used_memory")), 10, 64)
	rss, _ = strconv.ParseInt(strings.TrimSpace(infoField(addr, "memory", "used_memory_rss")), 10, 64)
	return
}

func memCmd(out []byte, args ...[]byte) []byte {
	out = appendArrayHdr(out, len(args))
	for _, a := range args {
		out = appendBulk(out, a)
	}
	return out
}

func elemBytes(prefix string, i int) []byte {
	return strconv.AppendInt([]byte(prefix), int64(i), 10)
}

func memScenarios() []memScenario {
	multi := func(cmd string, withVal bool) func([]byte, []byte, int, []byte) []byte {
		return func(out, key []byte, elems int, val []byte) []byte {
			n := 2 + elems
			if withVal {
				n = 2 + 2*elems
			}
			out = appendArrayHdr(out, n)
			out = appendBulkStr(out, cmd)
			out = appendBulk(out, key)
			for i := range elems {
				out = appendBulk(out, elemBytes("field:", i))
				if withVal {
					out = appendBulk(out, val)
				}
			}
			return out
		}
	}
	zadd := func(out, key []byte, elems int, val []byte) []byte {
		out = appendArrayHdr(out, 2+2*elems)
		out = appendBulkStr(out, "ZADD")
		out = appendBulk(out, key)
		for i := range elems {
			out = appendBulk(out, strconv.AppendInt(nil, int64(i), 10))
			out = appendBulk(out, elemBytes("member:", i))
		}
		return out
	}
	sadd := func(ints bool) func([]byte, []byte, int, []byte) []byte {
		return func(out, key []byte, elems int, val []byte) []byte {
			out = appendArrayHdr(out, 2+elems)
			out = appendBulkStr(out, "SADD")
			out = appendBulk(out, key)
			for i := range elems {
				if ints {
					out = appendBulk(out, strconv.AppendInt(nil, int64(i), 10))
				} else {
					out = appendBulk(out, elemBytes("member:", i))
				}
			}
			return out
		}
	}
	rpush := func(out, key []byte, elems int, val []byte) []byte {
		out = appendArrayHdr(out, 2+elems)
		out = appendBulkStr(out, "RPUSH")
		out = appendBulk(out, key)
		for range elems {
			out = appendBulk(out, val)
		}
		return out
	}
	set := func(out, key []byte, _ int, val []byte) []byte {
		return memCmd(out, []byte("SET"), key, val)
	}
	xadd := func(out, key []byte, elems int, val []byte) []byte {
		for range elems {
			out = memCmd(out, []byte("XADD"), key, []byte("*"), []byte("f"), val)
		}
		return out
	}
	return []memScenario{
		{"string 8B", 500_000, 1, 8, set, 1},
		{"string 16B", 500_000, 1, 16, set, 1},
		{"string 64B", 500_000, 1, 64, set, 1},
		{"hash small (10×16B)", 50_000, 10, 16, multi("HSET", true), 1},
		{"hash large (1000×16B)", 200, 1000, 16, multi("HSET", true), 1},
		{"list small (10×16B)", 50_000, 10, 16, rpush, 1},
		{"list large (10k×16B)", 100, 10_000, 16, rpush, 1},
		{"set small ints (10)", 50_000, 10, 0, sadd(true), 1},
		{"set large strs (10k)", 100, 10_000, 0, sadd(false), 1},
		{"zset small (10)", 50_000, 10, 0, zadd, 1},
		{"zset large (10k)", 100, 10_000, 0, zadd, 1},
		{"stream (1k × 16B)", 100, 1000, 16, xadd, 1000},
	}
}

func runMemory() {
	if len(addrs) > 1 {
		fmt.Println("── Memory Benchmark\n   (skipped — single-node only)")
		return
	}
	addr := addrs[0]
	fmt.Printf("── Memory Efficiency (%s) ─────────────────────\n", addr)
	fmt.Printf("  %-26s %9s %7s %12s %12s %12s\n", "scenario", "keys", "elems", "used B/key", "rss B/key", "used B/elem")
	for _, sc := range memScenarios() {
		was := skipFlush
		skipFlush = false
		flushServer()
		skipFlush = was
		time.Sleep(300 * time.Millisecond)
		baseUsed, baseRSS := memSample(addr)

		val := make([]byte, sc.vsize)
		for i := range val {
			val[i] = byte('a' + i%26)
		}
		loadScenario(sc, val)
		time.Sleep(300 * time.Millisecond)
		used, rss := memSample(addr)

		res := memResult{
			Scenario:   sc.name,
			Keys:       sc.keys,
			Elems:      sc.elems,
			ValueBytes: sc.vsize,
			UsedPerKey: float64(used-baseUsed) / float64(sc.keys),
			RSSPerKey:  float64(rss-baseRSS) / float64(sc.keys),
		}
		res.UsedPerElem = res.UsedPerKey / float64(sc.elems)
		payload := float64(sc.elems * (sc.vsize + 8))
		res.OverheadPerKey = res.UsedPerKey - payload
		memResults = append(memResults, res)
		fmt.Printf("  %-26s %9d %7d %12.1f %12.1f %12.2f\n",
			sc.name, sc.keys, sc.elems, res.UsedPerKey, res.RSSPerKey, res.UsedPerElem)
	}
	fmt.Println()
}

func loadScenario(sc memScenario, val []byte) {
	const conns = 16
	cs := preDial(conns)
	defer closeAll(cs)
	per := (sc.keys + conns - 1) / conns
	var wg sync.WaitGroup
	for ci, c := range cs {
		wg.Go(func() {
			r := bufio.NewReaderSize(c, 256<<10)
			var buf []byte
			var kb [48]byte
			lo, hi := ci*per, min((ci+1)*per, sc.keys)
			batch := max(1, 20_000/(sc.elems*sc.cmdsPerKey))
			for i := lo; i < hi; {
				n := min(batch, hi-i)
				buf = buf[:0]
				for j := range n {
					key := appendLen(append(kb[:0], "mem:"...), i+j)
					buf = sc.build(buf, key, sc.elems, val)
				}
				writeFull(c, buf)
				for range n * sc.cmdsPerKey {
					skipReply(r)
				}
				i += n
			}
		})
	}
	wg.Wait()
}
