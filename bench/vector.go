package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

func vecTopK(vecs []float32, q []float32, k int, keep func(int) bool) []int {
	type hit struct {
		id int
		d  float32
	}
	var qn float32
	for _, x := range q {
		qn += x * x
	}
	hits := make([]hit, 0, len(vecs)/VEC_DIM)
	for id := 0; id*VEC_DIM < len(vecs); id++ {
		if keep != nil && !keep(id) {
			continue
		}
		v := vecs[id*VEC_DIM : (id+1)*VEC_DIM]
		var dot, vn float32
		for i, x := range v {
			dot += x * q[i]
			vn += x * x
		}
		hits = append(hits, hit{id, 1 - dot/float32(math.Sqrt(float64(qn*vn)))})
	}
	slices.SortFunc(hits, func(a, b hit) int {
		switch {
		case a.d < b.d:
			return -1
		case a.d > b.d:
			return 1
		}
		return a.id - b.id
	})
	out := make([]int, 0, k)
	for _, h := range hits[:min(k, len(hits))] {
		out = append(out, h.id)
	}
	return out
}

func vecReplyIDs(reply any) []int {
	arr, _ := reply.([]any)
	var ids []int
	for i := 1; i < len(arr); i++ {
		key, ok := arr[i].(string)
		if !ok || !strings.HasPrefix(key, vecKeyPrefix) {
			continue
		}
		if id, err := strconv.Atoi(key[len(vecKeyPrefix):]); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func vecRecall(conn *net.TCPConn, r *bufio.Reader, cmd []byte, want []int) float64 {
	writeFull(conn, cmd)
	return recallAt(vecReplyIDs(readValue(r)), want, VEC_K)
}

const (
	VEC_DIM       = 768
	VEC_DOCS      = 50_000
	VEC_QUERIES   = 2_000
	VEC_K         = 10
	VEC_LOAD_PIPE = 200
	vecKeyPrefix  = "vec:"
	vecIndexName  = "vecbench"
)

func f32bytes(r *rand.Rand, buf []byte) []byte {
	for i := range VEC_DIM {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(r.Float32()*2-1))
	}
	return buf
}

func appendBulk(out []byte, b []byte) []byte {
	out = append(out, '$')
	out = appendLen(out, len(b))
	out = append(out, crlfB...)
	out = append(out, b...)
	return append(out, crlfB...)
}

func appendBulkStr(out []byte, s string) []byte {
	return appendBulk(out, []byte(s))
}

func appendArrayHdr(out []byte, n int) []byte {
	out = append(out, '*')
	out = appendLen(out, n)
	return append(out, crlfB...)
}

// skipReply skips one arbitrary RESP reply (array/bulk/int/simple/error).
func skipReply(r *bufio.Reader) {
	b, err := r.ReadByte()
	if err != nil {
		panic(err)
	}
	switch b {
	case '*', '~':
		line, err := r.ReadSlice('\n')
		if err != nil {
			panic(err)
		}
		n := 0
		for _, c := range line {
			if c < '0' || c > '9' {
				break
			}
			n = n*10 + int(c-'0')
		}
		for range n {
			skipReply(r)
		}
	case '$':
		line, err := r.ReadSlice('\n')
		if err != nil {
			panic(err)
		}
		if len(line) >= 2 && line[0] == '-' {
			return
		}
		vlen := 0
		for _, c := range line {
			if c >= '0' && c <= '9' {
				vlen = vlen*10 + int(c-'0')
			} else {
				break
			}
		}
		discardN(r, vlen+2)
	default:
		for {
			line, e := r.ReadSlice('\n')
			if e != nil && e != bufio.ErrBufferFull {
				panic(e)
			}
			if b == '-' {
				panic("server error: " + string(line))
			}
			if e != bufio.ErrBufferFull {
				break
			}
		}
	}
}

func vecCommand(conn *net.TCPConn, args ...[]byte) {
	out := appendArrayHdr(nil, len(args))
	for _, a := range args {
		out = appendBulk(out, a)
	}
	writeFull(conn, out)
}

func skipReplyTolerant(r *bufio.Reader) {
	if _, err := r.ReadByte(); err != nil {
		panic(err)
	}
	for {
		_, e := r.ReadSlice('\n')
		if e != nil && e != bufio.ErrBufferFull {
			panic(e)
		}
		if e != bufio.ErrBufferFull {
			return
		}
	}
}

func mustOK(r *bufio.Reader) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		panic(err)
	}
	if string(line) != "+OK\r\n" {
		panic("expected +OK, got " + string(line))
	}
}

// isRedisServer reports whether addr is a real Redis server: FyroDB's
// INFO server carries fyrodb_version, Redis only has redis_version.
func isRedisServer(addr string) bool {
	if infoField(addr, "server", "fyrodb_version") != "" {
		return false
	}
	return infoField(addr, "server", "redis_version") != ""
}

func runVector() {
	// OSS Redis Cluster can't shard a vector index, so for Redis we pin it to
	// one node (addrs[0]); for a FyroDB cluster we skip as before.
	if len(addrs) > 1 {
		if !isRedisServer(addrs[0]) {
			fmt.Println("── Vector / Search Benchmark\n   (skipped — single-node only)")
			return
		}
		fmt.Printf("── Vector / Search Benchmark\n   (Redis cluster — running against a single node %s)\n\n", addrs[0])
	}

	{
		conn, err := net.Dial("tcp", addrs[0])
		if err != nil {
			fmt.Println("── Vector / Search Benchmark\n   (skipped — could not connect)")
			return
		}
		// FT.INFO on a missing index: any reply confirms FT exists;
		// "unknown command" means it is not loaded.
		conn.Write([]byte("*2\r\n$7\r\nFT.INFO\r\n$12\r\n__ft_probe__\r\n"))
		probe := make([]byte, 256)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(probe)
		conn.Close()
		if n == 0 {
			fmt.Println("── Vector / Search Benchmark\n   (skipped — no response to FT.INFO probe)")
			return
		}
		reply := string(probe[:n])
		if len(reply) > 0 && reply[0] == '-' &&
			(strings.Contains(reply, "unknown command") || strings.Contains(reply, "ERR unknown")) {
			fmt.Println("── Vector / Search Benchmark\n   (skipped — FT module not loaded)")
			return
		}
	}

	flushServer()

	fmt.Printf("── Vector Benchmark (%s) ─────────────────────\n", addrs[0])
	fmt.Printf("dims=%d docs=%d queries=%d k=%d\n\n", VEC_DIM, VEC_DOCS, VEC_QUERIES, VEC_K)

	// 64-word vocabulary: a text term selects ~1/64 of the corpus, so the
	// text phases measure a selective predicate, not a match-all.
	words := []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
		"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
		"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
		"yankee", "zulu", "amber", "basalt", "cobalt", "dune", "ember", "flint",
		"granite", "harbor", "iris", "jade", "kelp", "lunar", "moss", "nectar",
		"onyx", "prism", "quartz", "reef", "sage", "tundra", "umber", "vellum",
		"willow", "xenon", "yarrow", "zephyr", "anchor", "beacon", "cinder", "drift",
		"edelweiss", "fathom", "gable", "helix", "ingot", "jasper", "kelvin", "lumen",
	}

	conn := dialTCP(0)
	defer conn.Close()
	r := bufio.NewReaderSize(conn, 1<<20)

	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName), []byte("DD"))
	skipReplyTolerant(r)
	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName+"H"), []byte("DD"))
	skipReplyTolerant(r)
	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName+"H32"), []byte("DD"))
	skipReplyTolerant(r)
	vecCommand(conn,
		[]byte("FT.CREATE"), []byte(vecIndexName),
		[]byte("ON"), []byte("HASH"),
		[]byte("PREFIX"), []byte("1"), []byte(vecKeyPrefix),
		[]byte("SCHEMA"),
		[]byte("title"), []byte("TEXT"),
		[]byte("category"), []byte("TAG"),
		[]byte("views"), []byte("NUMERIC"),
		[]byte("embedding"), []byte("VECTOR"), []byte("FLAT"), []byte("6"),
		[]byte("TYPE"), []byte("FLOAT32"),
		[]byte("DIM"), []byte(fmt.Sprintf("%d", VEC_DIM)),
		[]byte("DISTANCE_METRIC"), []byte("COSINE"),
	)
	mustOK(r)

	rng := rand.New(rand.NewPCG(42, 42))

	// ── Vector load (pipelined HSET with binary blobs) ──────────────────────
	var keyBuf [32]byte
	vecBuf := make([]byte, VEC_DIM*4)
	vecs := make([]float32, 0, VEC_DOCS*VEC_DIM)
	loadStart := time.Now()
	sent := 0
	out := make([]byte, 0, VEC_LOAD_PIPE*(VEC_DIM*4+128))
	for done := 0; done < VEC_DOCS; {
		out = out[:0]
		batch := min(VEC_LOAD_PIPE, VEC_DOCS-done)
		for j := range batch {
			id := done + j
			key := append(keyBuf[:0], vecKeyPrefix...)
			key = appendLen(key, id)
			f32bytes(rng, vecBuf)
			for i := range VEC_DIM {
				vecs = append(vecs, math.Float32frombits(binary.LittleEndian.Uint32(vecBuf[i*4:])))
			}
			out = appendArrayHdr(out, 10)
			out = appendBulkStr(out, "HSET")
			out = appendBulk(out, key)
			out = appendBulkStr(out, "title")
			out = appendBulkStr(out, "doc "+words[id%len(words)])
			out = appendBulkStr(out, "category")
			out = appendBulkStr(out, []string{"a", "b", "c"}[id%3])
			out = appendBulkStr(out, "views")
			out = appendBulkStr(out, "100")
			out = appendBulkStr(out, "embedding")
			out = appendBulk(out, vecBuf)
		}
		writeFull(conn, out)
		for range batch {
			line, err := r.ReadSlice('\n')
			if err != nil {
				panic(err)
			}
			if len(line) > 0 && line[0] == '-' {
				panic("HSET error: " + string(line))
			}
		}
		done += batch
		sent = done
	}
	loadElapsed := time.Since(loadStart)
	recordResult("Vector HSET load", int64(sent), loadElapsed)

	// ── KNN search ───────────────────────────────────────────────────────────
	qvec := make([]byte, VEC_DIM*4)
	qf := make([]float32, VEC_DIM)
	for i := range VEC_DIM {
		qf[i] = rng.Float32()*2 - 1
		binary.LittleEndian.PutUint32(qvec[i*4:], math.Float32bits(qf[i]))
	}
	gtAll := vecTopK(vecs, qf, VEC_K, nil)
	gtA := vecTopK(vecs, qf, VEC_K, func(id int) bool { return id%3 == 0 })
	vecs = nil
	efClause := ""
	efLabel := "server default"
	if ef := os.Getenv("VEC_EF"); ef != "" {
		efClause = " EF_RUNTIME " + ef
		efLabel = ef
	}
	recalls := map[string]float64{}
	buildSearchOn := func(idx, query string) []byte {
		out := appendArrayHdr(nil, 15)
		out = appendBulkStr(out, "FT.SEARCH")
		out = appendBulkStr(out, idx)
		out = appendBulkStr(out, query)
		out = appendBulkStr(out, "LIMIT")
		out = appendBulkStr(out, "0")
		out = appendBulkStr(out, "10")
		out = appendBulkStr(out, "DIALECT")
		out = appendBulkStr(out, "2")
		out = appendBulkStr(out, "RETURN")
		out = appendBulkStr(out, "1")
		out = appendBulkStr(out, "title")
		out = appendBulkStr(out, "PARAMS")
		out = appendBulkStr(out, "2")
		out = appendBulkStr(out, "v")
		out = appendBulk(out, qvec)
		return out
	}
	knnCmd := buildSearchOn(vecIndexName, "*=>[KNN 10 @embedding $v AS score]")

	vecCommand(conn, []byte("ECHO"), []byte("warm"))
	skipReply(r)
	for range 20 {
		writeFull(conn, knnCmd)
		skipReply(r)
	}

	knnStart := time.Now()
	for range VEC_QUERIES {
		writeFull(conn, knnCmd)
		skipReply(r)
	}
	knnElapsed := time.Since(knnStart)
	recordResult("Vector KNN-10", int64(VEC_QUERIES), knnElapsed)
	recalls["flat"] = vecRecall(conn, r, knnCmd, gtAll)

	// ── Filtered KNN (tag pre-filter) ───────────────────────────────────────
	fknnCmd := buildSearchOn(vecIndexName, "(@category:{a})=>[KNN 10 @embedding $v AS score]")
	for range 20 {
		writeFull(conn, fknnCmd)
		skipReply(r)
	}
	fknnStart := time.Now()
	for range VEC_QUERIES / 2 {
		writeFull(conn, fknnCmd)
		skipReply(r)
	}
	fknnElapsed := time.Since(fknnStart)
	recordResult("Vector KNN-10 +tag filter", int64(VEC_QUERIES/2), fknnElapsed)
	recalls["flat+tag"] = vecRecall(conn, r, fknnCmd, gtA)

	// ── Tag+numeric filter only ─────────────────────────────────────────────
	filCmd := buildSearchOn(vecIndexName, "@category:{a} @views:[50 150]")
	for range 20 {
		writeFull(conn, filCmd)
		skipReply(r)
	}
	filStart := time.Now()
	for range VEC_QUERIES / 2 {
		writeFull(conn, filCmd)
		skipReply(r)
	}
	filElapsed := time.Since(filStart)
	recordResult("Filter Tag+Num", int64(VEC_QUERIES/2), filElapsed)

	// ── Text term ───────────────────────────────────────────────────────────
	textCmd := func(idx, query string) []byte {
		out := appendArrayHdr(nil, 11)
		out = appendBulkStr(out, "FT.SEARCH")
		out = appendBulkStr(out, idx)
		out = appendBulkStr(out, query)
		out = appendBulkStr(out, "LIMIT")
		out = appendBulkStr(out, "0")
		out = appendBulkStr(out, "10")
		out = appendBulkStr(out, "DIALECT")
		out = appendBulkStr(out, "2")
		out = appendBulkStr(out, "RETURN")
		out = appendBulkStr(out, "1")
		out = appendBulkStr(out, "title")
		return out
	}
	termCmd := textCmd(vecIndexName, "@title:alpha")
	for range 20 {
		writeFull(conn, termCmd)
		skipReply(r)
	}
	termStart := time.Now()
	for range VEC_QUERIES / 2 {
		writeFull(conn, termCmd)
		skipReply(r)
	}
	termElapsed := time.Since(termStart)
	recordResult("Text term", int64(VEC_QUERIES/2), termElapsed)

	// ── Text + tag (mixed predicate) ────────────────────────────────────────
	ttCmd := textCmd(vecIndexName, "(@title:alpha @category:{a})")
	for range 20 {
		writeFull(conn, ttCmd)
		skipReply(r)
	}
	ttStart := time.Now()
	for range VEC_QUERIES / 2 {
		writeFull(conn, ttCmd)
		skipReply(r)
	}
	ttElapsed := time.Since(ttStart)
	recordResult("Text + Tag", int64(VEC_QUERIES/2), ttElapsed)

	// ── Negated text term ───────────────────────────────────────────────────
	negCmd := textCmd(vecIndexName, "-@title:alpha")
	for range 20 {
		writeFull(conn, negCmd)
		skipReply(r)
	}
	negStart := time.Now()
	for range VEC_QUERIES / 2 {
		writeFull(conn, negCmd)
		skipReply(r)
	}
	negElapsed := time.Since(negStart)
	recordResult("Text negation", int64(VEC_QUERIES/2), negElapsed)

	// ── Concurrent KNN (8 connections) ──────────────────────────────────────
	const concN = 8
	conns := make([]*net.TCPConn, concN)
	readers := make([]*bufio.Reader, concN)
	for i := range concN {
		conns[i] = dialTCP(100 + i)
		readers[i] = bufio.NewReaderSize(conns[i], 1<<20)
		writeFull(conns[i], knnCmd)
		skipReply(readers[i])
	}
	var wg sync.WaitGroup
	perClient := VEC_QUERIES / concN
	concStart := time.Now()
	for i := range concN {
		wg.Go(func() {
			for range perClient {
				writeFull(conns[i], knnCmd)
				skipReply(readers[i])
			}
		})
	}
	wg.Wait()
	concElapsed := time.Since(concStart)
	for _, c := range conns {
		c.Close()
	}
	recordResult("Vector KNN-10 (8 clients)", int64(perClient*concN), concElapsed)

	// ── HNSW index: build once, query fast ──────────────────────────────────
	// Build time: FT.CREATE until FT.INFO reports indexing finished.
	createHnsw := func(name string, m, ef int) time.Duration {
		vecCommand(conn,
			[]byte("FT.CREATE"), []byte(name),
			[]byte("ON"), []byte("HASH"),
			[]byte("PREFIX"), []byte("1"), []byte(vecKeyPrefix),
			[]byte("SCHEMA"),
			[]byte("embedding"), []byte("VECTOR"), []byte("HNSW"), []byte("10"),
			[]byte("TYPE"), []byte("FLOAT32"),
			[]byte("DIM"), []byte(fmt.Sprintf("%d", VEC_DIM)),
			[]byte("DISTANCE_METRIC"), []byte("COSINE"),
			[]byte("M"), []byte(fmt.Sprintf("%d", m)),
			[]byte("EF_CONSTRUCTION"), []byte(fmt.Sprintf("%d", ef)),
		)
		start := time.Now()
		mustOK(r)
		// FT.CREATE returns at once and indexes existing docs in the
		// background; the build ends when FT.INFO stops reporting indexing.
		waitIndexed(conn, r, name)
		return time.Since(start)
	}
	hBuildElapsed := createHnsw(vecIndexName+"H", 16, 100)

	hnnCmd := buildSearchOn(vecIndexName+"H", "*=>[KNN 10 @embedding $v"+efClause+" AS score]")
	for i := 20; i >= 0; i-- {
		writeFull(conn, hnnCmd)
		skipReply(r)
	}
	hnnStart := time.Now()
	for range VEC_QUERIES {
		writeFull(conn, hnnCmd)
		skipReply(r)
	}
	hnnElapsed := time.Since(hnnStart)
	recordResult("Vector HNSW KNN-10", int64(VEC_QUERIES), hnnElapsed)
	recalls["hnsw"] = vecRecall(conn, r, hnnCmd, gtAll)

	// Same corpus, doubled connectivity: M=32 doubles level-0 edges and
	// EF_CONSTRUCTION=200 widens the candidate search during the build.
	h32BuildElapsed := createHnsw(vecIndexName+"H32", 32, 200)
	h32Cmd := buildSearchOn(vecIndexName+"H32", "*=>[KNN 10 @embedding $v"+efClause+" AS score]")
	for range 20 {
		writeFull(conn, h32Cmd)
		skipReply(r)
	}
	h32Start := time.Now()
	for range VEC_QUERIES {
		writeFull(conn, h32Cmd)
		skipReply(r)
	}
	h32Elapsed := time.Since(h32Start)
	recordResult("Vector HNSW M=32 KNN-10", int64(VEC_QUERIES), h32Elapsed)
	recalls["hnsw M=32"] = vecRecall(conn, r, h32Cmd, gtAll)

	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName), []byte("DD"))
	mustOK(r)
	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName+"H"), []byte("DD"))
	mustOK(r)
	vecCommand(conn, []byte("FT.DROPINDEX"), []byte(vecIndexName+"H32"), []byte("DD"))
	mustOK(r)

	fmt.Println("\n── Vector Summary ──────────────────────────────")
	fmt.Printf("load:              %s docs/s\n", fmtRate(float64(VEC_DOCS)/loadElapsed.Seconds()))
	fmt.Printf("hnsw build:        %.1fs (one-time, inside FT.CREATE)\n", hBuildElapsed.Seconds())
	fmt.Printf("hnsw M=32 build:   %.1fs\n", h32BuildElapsed.Seconds())
	fmt.Printf("hnsw-10 latency:   %.3f ms/q\n", hnnElapsed.Seconds()*1000/float64(VEC_QUERIES))
	fmt.Printf("hnsw M=32 latency: %.3f ms/q\n", h32Elapsed.Seconds()*1000/float64(VEC_QUERIES))
	fmt.Printf("knn-10 latency:    %.2f ms/q\n", knnElapsed.Seconds()*1000/float64(VEC_QUERIES))
	fmt.Printf("filtered latency:  %.2f ms/q\n", fknnElapsed.Seconds()*1000/float64(VEC_QUERIES/2))
	fmt.Printf("text latency:      %.3f ms/q\n", termElapsed.Seconds()*1000/float64(VEC_QUERIES/2))
	fmt.Printf("conc. throughput:  %s q/s\n", fmtRate(float64(perClient*concN)/concElapsed.Seconds()))
	fmt.Printf("hnsw ef_runtime:   %s\n", efLabel)
	for _, k := range []string{"flat", "flat+tag", "hnsw", "hnsw M=32"} {
		fmt.Printf("recall@10 %-9s %.3f\n", k+":", recalls[k])
	}
}
