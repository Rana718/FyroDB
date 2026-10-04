package main

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ANN recall/QPS benchmark (`-m ann`). Loads a TEXMEX-format dataset
// (SIFT-1M, GIST, GloVe/dbpedia converted to .fvecs) or generates a
// clustered synthetic corpus, computes exact ground truth when needed, and
// reports recall@k vs throughput for an EF_RUNTIME sweep, a concurrency sweep
// and a filter-selectivity sweep. Filtered ground truth is always computed
// here, over the exact filtered subset.
type annConfig struct {
	dir       string
	metric    string // L2 | COSINE | IP
	algo      string // HNSW | FLAT
	n         int    // base vectors to load (0 = all)
	q         int    // queries
	k         int
	m         int
	efc       int
	efs       []int
	conc      []int
	filters   []int // selectivity percentages
	synthN    int
	synthDim  int
	quantType string
	quant     string // HNSW QUANTIZATION (empty = server default)
}

var annCfg = annConfig{
	metric:    "L2",
	algo:      "HNSW",
	q:         1000,
	k:         10,
	m:         16,
	efc:       200,
	efs:       []int{10, 20, 40, 80, 160, 320},
	conc:      []int{1, 8, 32},
	filters:   []int{1, 10, 50},
	synthN:    100_000,
	synthDim:  128,
	quantType: "FLOAT32",
}

func parseIntList(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

// readFvecs reads TEXMEX .fvecs: per vector a little-endian int32 dim then
// dim float32s. limit=0 reads everything.
func readFvecs(path string, limit int) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var out [][]float32
	var hdr [4]byte
	for limit == 0 || len(out) < limit {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		d := int(binary.LittleEndian.Uint32(hdr[:]))
		buf := make([]byte, d*4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		v := make([]float32, d)
		for i := range v {
			v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
		}
		out = append(out, v)
	}
	return out, nil
}

func readIvecs(path string, limit int) ([][]int32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var out [][]int32
	var hdr [4]byte
	for limit == 0 || len(out) < limit {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		d := int(binary.LittleEndian.Uint32(hdr[:]))
		buf := make([]byte, d*4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		v := make([]int32, d)
		for i := range v {
			v[i] = int32(binary.LittleEndian.Uint32(buf[i*4:]))
		}
		out = append(out, v)
	}
	return out, nil
}

func findFile(dir, suffix string) string {
	m, _ := filepath.Glob(filepath.Join(dir, "*"+suffix))
	if len(m) > 0 {
		return m[0]
	}
	return ""
}

// synthClustered draws vectors around a few hundred Gaussian centroids:
// unlike uniform noise this has the neighborhood structure real embeddings
// have, so recall numbers mean something.
func synthClustered(n, q, dim int) (base, queries [][]float32) {
	r := rand.New(rand.NewPCG(7, 11))
	nc := 256
	cents := make([][]float32, nc)
	for i := range cents {
		c := make([]float32, dim)
		for j := range c {
			c[j] = float32(r.NormFloat64())
		}
		cents[i] = c
	}
	gen := func(cnt int) [][]float32 {
		out := make([][]float32, cnt)
		for i := range out {
			c := cents[r.IntN(nc)]
			v := make([]float32, dim)
			for j := range v {
				v[j] = c[j] + float32(r.NormFloat64()*0.35)
			}
			out[i] = v
		}
		return out
	}
	return gen(n), gen(q)
}

func annDist(metric string, a, b []float32) float32 {
	switch metric {
	case "IP":
		var s float32
		for i := range a {
			s += a[i] * b[i]
		}
		return -s
	case "COSINE":
		var s, na, nb float32
		for i := range a {
			s += a[i] * b[i]
			na += a[i] * a[i]
			nb += b[i] * b[i]
		}
		return 1 - s/float32(math.Sqrt(float64(na)*float64(nb))+1e-20)
	default:
		var s float32
		for i := range a {
			d := a[i] - b[i]
			s += d * d
		}
		return s
	}
}

type distID struct {
	d  float32
	id int
}
type maxHeap []distID

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].d > h[j].d }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(distID)) }
func (h *maxHeap) Pop() any {
	o := *h
	x := o[len(o)-1]
	*h = o[:len(o)-1]
	return x
}

// groundTruth is exact top-k per query, restricted to ids with allow(id).
func groundTruth(metric string, base, queries [][]float32, k int, allow func(int) bool) [][]int {
	gt := make([][]int, len(queries))
	var wg sync.WaitGroup
	work := make(chan int, len(queries))
	for i := range queries {
		work <- i
	}
	close(work)
	for range runtime.NumCPU() {
		wg.Go(func() {
			for qi := range work {
				h := make(maxHeap, 0, k+1)
				for id, v := range base {
					if allow != nil && !allow(id) {
						continue
					}
					d := annDist(metric, queries[qi], v)
					if len(h) < k {
						heap.Push(&h, distID{d, id})
					} else if d < h[0].d {
						h[0] = distID{d, id}
						heap.Fix(&h, 0)
					}
				}
				sort.Slice(h, func(a, b int) bool { return h[a].d < h[b].d })
				ids := make([]int, len(h))
				for i, e := range h {
					ids[i] = e.id
				}
				gt[qi] = ids
			}
		})
	}
	wg.Wait()
	return gt
}

func f32blob(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(x))
	}
	return b
}

// readSearchIDs parses an FT.SEARCH NOCONTENT reply into doc ids ("ann:<id>").
func readSearchIDs(r *bufio.Reader, dst []int) []int {
	dst = dst[:0]
	line, err := r.ReadSlice('\n')
	if err != nil {
		panic(err)
	}
	if line[0] == '-' {
		panic("search error: " + string(line))
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(line[1:])))
	if n == 0 {
		return dst
	}
	skipReply(r) // total count
	for range n - 1 {
		hdr, err := r.ReadSlice('\n')
		if err != nil {
			panic(err)
		}
		l, _ := strconv.Atoi(strings.TrimSpace(string(hdr[1:])))
		body := make([]byte, l+2)
		if _, err := io.ReadFull(r, body); err != nil {
			panic(err)
		}
		key := string(body[:l])
		if i := strings.LastIndexByte(key, ':'); i >= 0 {
			if id, err := strconv.Atoi(key[i+1:]); err == nil {
				dst = append(dst, id)
			}
		}
	}
	return dst
}

func recallAt(got []int, want []int, k int) float64 {
	if len(want) == 0 {
		return 1
	}
	want = want[:min(k, len(want))]
	set := make(map[int]struct{}, len(want))
	for _, w := range want {
		set[w] = struct{}{}
	}
	hit := 0
	for _, g := range got {
		if _, ok := set[g]; ok {
			hit++
		}
	}
	return float64(hit) / float64(len(want))
}

func annSearchCmd(idx string, k, ef int, filter string, blob []byte) []byte {
	query := fmt.Sprintf("*=>[KNN %d @vec $v EF_RUNTIME %d AS score]", k, ef)
	if filter != "" {
		query = fmt.Sprintf("(%s)=>[KNN %d @vec $v EF_RUNTIME %d AS score]", filter, k, ef)
	}
	out := appendArrayHdr(nil, 13)
	out = appendBulkStr(out, "FT.SEARCH")
	out = appendBulkStr(out, idx)
	out = appendBulkStr(out, query)
	out = appendBulkStr(out, "NOCONTENT")
	out = appendBulkStr(out, "LIMIT")
	out = appendBulkStr(out, "0")
	out = appendBulkStr(out, strconv.Itoa(k))
	out = appendBulkStr(out, "DIALECT")
	out = appendBulkStr(out, "2")
	out = appendBulkStr(out, "PARAMS")
	out = appendBulkStr(out, "2")
	out = appendBulkStr(out, "v")
	return appendBulk(out, blob)
}

// annQueryPhase runs all queries over `conc` connections and reports QPS,
// latency percentiles and mean recall@k.
func annQueryPhase(label, idx string, cfg annConfig, ef, conc int, filter string,
	blobs [][]byte, gt [][]int) latencySummary {
	conns := preDial(conc)
	defer closeAll(conns)
	cmds := make([][]byte, len(blobs))
	for i, b := range blobs {
		cmds[i] = annSearchCmd(idx, cfg.k, ef, filter, b)
	}
	hs := make([]hist, conc)
	recalls := make([]float64, conc)
	counts := make([]int, conc)
	start := time.Now()
	var wg sync.WaitGroup
	for ci, c := range conns {
		wg.Go(func() {
			r := bufio.NewReaderSize(c, 1<<20)
			var ids []int
			for qi := ci; qi < len(cmds); qi += conc {
				t0 := time.Now()
				writeFull(c, cmds[qi])
				ids = readSearchIDs(r, ids)
				hs[ci].record(time.Since(t0))
				if qi == 0 && os.Getenv("ANN_DEBUG") != "" {
					fmt.Printf("   debug q0 got=%v want=%v\n", ids, gt[qi])
					writeFull(c, cmds[qi])
					fmt.Printf("   debug raw reply: %v\n", readValue(r))
				}
				recalls[ci] += recallAt(ids, gt[qi], cfg.k)
				counts[ci]++
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	var total hist
	var rsum float64
	var n int
	for i := range hs {
		total.merge(&hs[i])
		rsum += recalls[i]
		n += counts[i]
	}
	s := summarize(label, &total, elapsed)
	s.Recall = rsum / float64(max(n, 1))
	printLatency(s)
	latencyResults = append(latencyResults, s)
	return s
}

// ftInfoField reads one top-level field of FT.INFO as a string.
func ftInfoField(conn *net.TCPConn, r *bufio.Reader, idx, field string) string {
	vecCommand(conn, []byte("FT.INFO"), []byte(idx))
	reply := readValue(r)
	arr, _ := reply.([]any)
	for i := 0; i+1 < len(arr); i += 2 {
		if k, ok := arr[i].(string); ok && k == field {
			switch v := arr[i+1].(type) {
			case string:
				return v
			case int64:
				return strconv.FormatInt(v, 10)
			}
		}
	}
	return ""
}

func waitIndexed(conn *net.TCPConn, r *bufio.Reader, idx string) {
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		indexing := ftInfoField(conn, r, idx, "indexing")
		pct, err := strconv.ParseFloat(ftInfoField(conn, r, idx, "percent_indexed"), 64)
		if indexing != "1" && (err != nil || pct >= 1) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("indexing did not finish before the benchmark deadline: " + idx)
}

// readValue decodes one RESP2 reply into string / int64 / []any / nil.
func readValue(r *bufio.Reader) any {
	line, err := r.ReadSlice('\n')
	if err != nil {
		panic(err)
	}
	body := strings.TrimRight(string(line[1:]), "\r\n")
	switch line[0] {
	case '*':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = readValue(r)
		}
		return out
	case '$':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return nil
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(r, b); err != nil {
			panic(err)
		}
		return string(b[:n])
	case ':':
		v, _ := strconv.ParseInt(body, 10, 64)
		return v
	case '-':
		return fmt.Errorf("%s", body)
	default:
		return body
	}
}

func runANN() {
	cfg := annCfg
	if len(addrs) > 1 {
		fmt.Println("── ANN Benchmark\n   (skipped — single-node only)")
		return
	}
	var base, queries [][]float32
	var gt [][]int
	src := "synthetic-clustered"
	if cfg.dir != "" {
		bp, qp, gp := findFile(cfg.dir, "base.fvecs"), findFile(cfg.dir, "query.fvecs"), findFile(cfg.dir, "groundtruth.ivecs")
		if bp == "" || qp == "" {
			fmt.Fprintf(os.Stderr, "ann: need *base.fvecs and *query.fvecs in %s\n", cfg.dir)
			os.Exit(1)
		}
		var err error
		if base, err = readFvecs(bp, cfg.n); err != nil {
			panic(err)
		}
		if queries, err = readFvecs(qp, cfg.q); err != nil {
			panic(err)
		}
		// Shipped ground truth is only valid for the full base set.
		if gp != "" && cfg.n == 0 {
			raw, err := readIvecs(gp, cfg.q)
			if err != nil {
				panic(err)
			}
			gt = make([][]int, len(raw))
			for i, row := range raw {
				for _, id := range row[:min(cfg.k, len(row))] {
					gt[i] = append(gt[i], int(id))
				}
			}
		}
		src = filepath.Base(bp)
	} else {
		base, queries = synthClustered(cfg.synthN, cfg.q, cfg.synthDim)
	}
	dim := len(base[0])
	fmt.Printf("── ANN Benchmark (%s) ───────────────────────────\n", addrs[0])
	fmt.Printf("dataset=%s n=%d dim=%d queries=%d k=%d metric=%s algo=%s M=%d EF_C=%d type=%s quant=%s\n\n",
		src, len(base), dim, len(queries), cfg.k, cfg.metric, cfg.algo, cfg.m, cfg.efc, cfg.quantType, cfg.quant)

	if gt == nil {
		t0 := time.Now()
		gt = groundTruth(cfg.metric, base, queries, cfg.k, nil)
		fmt.Printf("   ground truth: computed in %s\n\n", time.Since(t0).Round(time.Millisecond))
	}

	flushServer()
	conn := dialTCP(0)
	defer conn.Close()
	r := bufio.NewReaderSize(conn, 1<<20)
	const idx = "annbench"
	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(idx), []byte("DD"))
	skipReplyTolerant(r)

	args := [][]byte{
		[]byte("FT.CREATE"), []byte(idx), []byte("ON"), []byte("HASH"),
		[]byte("PREFIX"), []byte("1"), []byte("ann:"), []byte("SCHEMA"),
		[]byte("bucket"), []byte("NUMERIC"),
		[]byte("vec"), []byte("VECTOR"), []byte(cfg.algo),
	}
	vargs := [][]byte{
		[]byte("TYPE"), []byte(cfg.quantType), []byte("DIM"), []byte(strconv.Itoa(dim)),
		[]byte("DISTANCE_METRIC"), []byte(cfg.metric),
	}
	if cfg.algo == "HNSW" {
		vargs = append(vargs, []byte("M"), []byte(strconv.Itoa(cfg.m)),
			[]byte("EF_CONSTRUCTION"), []byte(strconv.Itoa(cfg.efc)))
		if cfg.quant != "" {
			vargs = append(vargs, []byte("QUANTIZATION"), []byte(cfg.quant))
		}
	}
	args = append(args, []byte(strconv.Itoa(len(vargs))))
	args = append(args, vargs...)
	vecCommand(conn, args...)
	mustOK(r)

	// Load: every doc carries bucket = id % 100 so `@bucket:[0 p-1]`
	// selects exactly p% of the corpus for the filter sweep.
	loadStart := time.Now()
	const loadConns = 8
	lc := preDial(loadConns)
	var wg sync.WaitGroup
	for ci, c := range lc {
		wg.Go(func() {
			rr := bufio.NewReaderSize(c, 256<<10)
			var out []byte
			var kb [32]byte
			pending := 0
			for id := ci; id < len(base); id += loadConns {
				key := appendLen(append(kb[:0], "ann:"...), id)
				out = appendArrayHdr(out, 6)
				out = appendBulkStr(out, "HSET")
				out = appendBulk(out, key)
				out = appendBulkStr(out, "bucket")
				out = appendBulkStr(out, strconv.Itoa(id%100))
				out = appendBulkStr(out, "vec")
				out = appendBulk(out, f32blob(base[id]))
				pending++
				if pending == 256 {
					writeFull(c, out)
					for range pending {
						skipReply(rr)
					}
					out, pending = out[:0], 0
				}
			}
			if pending > 0 {
				writeFull(c, out)
				for range pending {
					skipReply(rr)
				}
			}
		})
	}
	wg.Wait()
	closeAll(lc)
	loadElapsed := time.Since(loadStart)
	recordResult("ANN HSET load", int64(len(base)), loadElapsed)

	blobs := make([][]byte, len(queries))
	for i, q := range queries {
		blobs[i] = f32blob(q)
	}

	// The first query kicks the build if the load outran it; then wait for
	// FT.INFO to report the index complete so no timed query hits a
	// partial graph.
	buildStart := time.Now()
	writeFull(conn, annSearchCmd(idx, cfg.k, 10, "", blobs[0]))
	readSearchIDs(r, nil)
	waitIndexed(conn, r, idx)
	buildElapsed := time.Since(buildStart)
	fmt.Printf("   index build (after load): %s\n", buildElapsed.Round(time.Millisecond))
	recordResult("ANN index build", int64(len(base)), buildElapsed)
	used, rss := memSample(addrs[0])
	fmt.Printf("   memory after load+build: used=%s rss=%s\n\n", fmtBytes(used), fmtBytes(rss))

	fmt.Println("── EF_RUNTIME sweep (1 client)")
	for _, ef := range cfg.efs {
		annQueryPhase(fmt.Sprintf("ANN ef=%d", ef), idx, cfg, ef, 1, "", blobs, gt)
	}
	pickEF := cfg.efs[len(cfg.efs)/2]
	fmt.Printf("── Concurrency sweep (ef=%d)\n", pickEF)
	for _, c := range cfg.conc {
		annQueryPhase(fmt.Sprintf("ANN ef=%d conc=%d", pickEF, c), idx, cfg, pickEF, c, "", blobs, gt)
	}
	fmt.Printf("── Filter selectivity sweep (ef=%d)\n", pickEF)
	for _, pct := range cfg.filters {
		allow := func(id int) bool { return id%100 < pct }
		fgt := groundTruth(cfg.metric, base, queries, cfg.k, allow)
		filter := fmt.Sprintf("@bucket:[0 %d]", pct-1)
		annQueryPhase(fmt.Sprintf("ANN filter=%d%% ef=%d", pct, pickEF), idx, cfg, pickEF, 1, filter, blobs, fgt)
	}

	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(idx), []byte("DD"))
	skipReplyTolerant(r)
}
