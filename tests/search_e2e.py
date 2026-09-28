#!/usr/bin/env python3
"""End-to-end checks for incremental index maintenance and persistence.

    python3 tests/search_e2e.py [path/to/fyro_db]

Starts its own server (twice: before and after a SAVE/restart).
"""
import os
import random
import socket
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(__file__))
from blocking_e2e import Client, check  # noqa: E402

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.environ.get("CARGO_TARGET_DIR", "target"), "release", "fyro_db")
PORT = int(os.environ.get("FYRODB_PORT", "8000"))
DIM = 16


def start(rdb):
    p = subprocess.Popen([BIN], env=dict(os.environ, FYRODB_RDB_PATH=rdb, FYRODB_PORT=str(PORT)),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", PORT)).close()
            return p
        except OSError:
            time.sleep(0.05)
    raise SystemExit("server did not start")


def blob(v):
    return struct.pack("<%df" % DIM, *v)


def info(c, idx):
    r = c.call("FT.INFO", idx)
    return {r[i]: r[i + 1] for i in range(0, len(r) - 1, 2)}


def wait_indexed(c, idx):
    for _ in range(600):
        if info(c, idx).get("indexing") != "1":
            return
        time.sleep(0.05)
    raise SystemExit("index never finished building")


def m_quant(c, idx):
    attrs = info(c, idx)["attributes"]
    a = [a for a in attrs if "vec" in a][0]
    return dict(zip(a[0::2], a[1::2])).get("quantization")


def knn(c, idx, v, k=5, flt="*"):
    q = "(%s)=>[KNN %d @vec $v EF_RUNTIME 64 AS s]" % (flt, k) if flt != "*" else "*=>[KNN %d @vec $v EF_RUNTIME 64 AS s]" % k
    r = c.call("FT.SEARCH", idx, q, "NOCONTENT", "LIMIT", "0", str(k), "DIALECT", "2", "PARAMS", "2", "v", blob(v))
    return r[1:]


def main():
    tmp = tempfile.mkdtemp()
    rdb = os.path.join(tmp, "s.rdb")
    srv = start(rdb)
    try:
        c = Client()
        c.call("FLUSHALL")
        check(c.call("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA",
                     "tag", "TAG", "n", "NUMERIC",
                     "vec", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32", "DIM", str(DIM),
                     "DISTANCE_METRIC", "L2", "M", "24", "EF_CONSTRUCTION", "150") == "OK", "FT.CREATE")
        rnd = random.Random(3)
        vecs = {}
        for i in range(3000):
            v = [rnd.gauss(0, 1) for _ in range(DIM)]
            vecs[i] = v
            c.send("HSET", "d:%d" % i, "tag", "t%d" % (i % 3), "n", str(i), "vec", blob(v))
        for _ in range(3000):
            c.read()
        knn(c, "idx", vecs[0])  # kicks the build if FT.CREATE's saw an empty keyspace
        wait_indexed(c, "idx")
        check(knn(c, "idx", vecs[7])[0] == "d:7", "KNN finds a loaded doc")

        # Read-your-writes through the dirty set (no keyspace rescan).
        far = [50.0] * DIM
        c.call("HSET", "d:new", "tag", "fresh", "n", "5", "vec", blob(far))
        check(knn(c, "idx", far)[0] == "d:new", "a new doc is visible to the very next KNN")
        moved = [-50.0] * DIM
        c.call("HSET", "d:new", "vec", blob(moved))
        check(knn(c, "idx", far, k=1) != ["d:new"], "a rewritten vector leaves its old position")
        check(knn(c, "idx", moved, k=1) == ["d:new"], "rewritten vector found at its new position")
        c.call("HDEL", "d:new", "vec")
        check("d:new" not in knn(c, "idx", moved, k=3), "removing the vector field drops the doc")
        c.call("HSET", "d:new", "vec", blob(moved))
        c.call("DEL", "d:new")
        check("d:new" not in knn(c, "idx", moved, k=3), "DEL drops the doc")

        # Filter postings update incrementally too.
        r = c.call("FT.SEARCH", "idx", "@tag:{fresh}", "NOCONTENT")
        check(r[0] == 0, "deleted doc left the tag postings")
        c.call("HSET", "d:5", "tag", "fresh")
        r = c.call("FT.SEARCH", "idx", "@tag:{fresh}", "NOCONTENT")
        check(r[1:] == ["d:5"], "tag rewrite visible immediately")
        r = c.call("FT.SEARCH", "idx", "@tag:{t2}", "NOCONTENT", "LIMIT", "0", "0")
        check(r[0] == 999, "d:5's old tag posting retired (%s)" % r[0])
        check(knn(c, "idx", vecs[5], k=1, flt="@tag:{fresh}") == ["d:5"], "filtered KNN sees the new tag")
        c.call("HSET", "d:9", "n", "100000")
        r = c.call("FT.SEARCH", "idx", "@n:[99999 +inf]", "NOCONTENT")
        check(r[1:] == ["d:9"], "numeric rewrite visible immediately")

        # Element types and quantization modes.
        check(m_quant(c, "idx") == "NONE", "16-dim HNSW defaults to exact f32 traversal")
        for q in ("NONE", "RABITQ"):
            name = "q" + q
            check(c.call("FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "vec", "VECTOR", "HNSW", "8",
                         "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2", "QUANTIZATION", q) == "OK", "FT.CREATE " + q)
            knn(c, name, vecs[0])
            wait_indexed(c, name)
            check(m_quant(c, name) == q, q + " reported by FT.INFO")
            check(knn(c, name, vecs[11], k=1) == ["d:11"], q + " finds the exact match")
            c.call("FT.DROPINDEX", name)
        check(c.call("FT.CREATE", "h16", "ON", "HASH", "PREFIX", "1", "h:", "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                     "TYPE", "FLOAT16", "DIM", "4", "DISTANCE_METRIC", "L2") == "OK", "FT.CREATE FLOAT16")
        half = lambda xs: struct.pack("<4e", *xs)
        for i, v in enumerate([[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0.9, 0.1, 0, 0]]):
            c.call("HSET", "h:%d" % i, "vec", half(v))
        r = c.call("FT.SEARCH", "h16", "*=>[KNN 2 @vec $v AS s]", "NOCONTENT", "DIALECT", "2", "PARAMS", "2", "v", half([1, 0, 0, 0]))
        check(r[1:] == ["h:0", "h:3"], "FLOAT16 documents and query decode (%s)" % r[1:])
        r = c.call("FT.SEARCH", "h16", "*=>[KNN 1 @vec $v AS s]", "NOCONTENT", "DIALECT", "2", "PARAMS", "2", "v", struct.pack("<4f", 1, 0, 0, 0))
        check(isinstance(r, Exception), "a FLOAT32 blob against a FLOAT16 field is rejected")

        # VECTOR_RANGE through the index (plain and filtered) vs exact.
        import math
        q = vecs[77]
        dists = sorted((math.dist(q, v), i) for i, v in vecs.items())
        radius = dists[40][0]
        want = {"d:%d" % i for d, i in dists if d <= radius}
        r = c.call("FT.SEARCH", "idx", "@vec:[VECTOR_RANGE $r $v]", "NOCONTENT", "LIMIT", "0", "1000",
                   "DIALECT", "2", "PARAMS", "4", "r", str(radius), "v", blob(q))
        got = set(r[1:])
        check(got <= want and len(got) >= 0.95 * len(want), "VECTOR_RANGE via index (%d of %d)" % (len(got), len(want)))
        r = c.call("FT.SEARCH", "idx", "@tag:{t1} @vec:[VECTOR_RANGE $r $v]", "NOCONTENT", "LIMIT", "0", "1000",
                   "DIALECT", "2", "PARAMS", "4", "r", str(radius), "v", blob(q))
        wantf = {k for k in want if int(k[2:]) % 3 == 1 and k != "d:5"}
        check(set(r[1:]) == wantf, "filtered VECTOR_RANGE exact on a small allow-list")

        before = knn(c, "idx", vecs[42], k=10)
        check(c.call("SAVE") == "OK", "SAVE")
    finally:
        srv.terminate()
        srv.wait()

    srv = start(rdb)
    try:
        c = Client()
        attrs = info(c, "idx")["attributes"]
        vec_attr = [a for a in attrs if "vec" in a][0]
        m = dict(zip(vec_attr[0::2], vec_attr[1::2]))
        check(m.get("M") == "24" and m.get("ef_construction") == "150", "M / EF_CONSTRUCTION survive restart")
        check(info(c, "idx").get("indexing") == "0", "restored graph needs no reindex")
        after = knn(c, "idx", vecs[42], k=10)
        check(after == before, "restored graph answers like before the restart")
        c.call("HSET", "d:post", "tag", "x", "n", "1", "vec", blob(far))
        check(knn(c, "idx", far, k=1) == ["d:post"], "writes after restart are indexed")
        print("all search checks passed")
    finally:
        srv.terminate()
        srv.wait()


if __name__ == "__main__":
    main()
