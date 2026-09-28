#!/usr/bin/env python3
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(__file__))
from blocking_e2e import Client, check  # noqa: E402
from search_e2e import start  # noqa: E402

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.environ.get("CARGO_TARGET_DIR", "target"), "release", "fyro_db")


def main():
    import tempfile
    srv = start(os.path.join(tempfile.mkdtemp(), "t.rdb"))
    try:
        c = Client()
        c.call("FLUSHALL")
        c.call("FT.CREATE", "docs", "ON", "HASH", "PREFIX", "1", "p:", "SCHEMA",
               "title", "TEXT", "body", "TEXT", "cat", "TAG", "price", "NUMERIC",
               "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")
        docs = [
            ("p:1", "Rust database engine", "a fast engine written in rust rust rust", "db", 10, (1, 0)),
            ("p:2", "Cooking pasta", "the pasta recipe with tomato", "food", 5, (0, 1)),
            ("p:3", "Rust for beginners", "learn rust", "book", 20, (0.9, 0.1)),
            ("p:4", "Database internals", "storage engines and indexes", "book", 30, (0.8, 0.2)),
            ("p:5", "Tomato soup", "a soup of tomato", "food", 7, (0.1, 0.9)),
        ]
        for key, title, body, cat, price, v in docs:
            c.call("HSET", key, "title", title, "body", body, "cat", cat, "price", str(price), "vec", struct.pack("<2f", *v))

        r = c.call("FT.SEARCH", "docs", "@body:rust", "NOCONTENT", "WITHSCORES")
        check(r[0] == 2 and r[1] == "p:1", "BM25 ranks the doc with more 'rust' first (%s)" % r)
        check(float(r[2]) > float(r[4]) > 0, "scores descend")
        r = c.call("FT.SEARCH", "docs", "rust", "NOCONTENT")
        check(set(r[1:]) == {"p:1", "p:3"}, "unfielded term searches every TEXT field (%s)" % r)
        r = c.call("FT.SEARCH", "docs", "tomato soup", "NOCONTENT")
        check(r[1:] == ["p:5"], "terms are ANDed across fields")
        r = c.call("FT.SEARCH", "docs", "@body:(the tomato)", "NOCONTENT")
        check(set(r[1:]) == {"p:2", "p:5"}, "stopwords are ignored (%s)" % r)
        r = c.call("FT.SEARCH", "docs", "@title:RUST", "NOCONTENT")
        check(set(r[1:]) == {"p:1", "p:3"}, "case-insensitive")

        q = struct.pack("<2f", 1, 0)
        r = c.call("FT.HYBRID", "docs", "SEARCH", "@body:rust", "VSIM", "@vec", "$v", "KNN", "3",
                   "COMBINE", "RRF", "CONSTANT", "60", "LIMIT", "0", "3", "NOCONTENT", "WITHSCORES", "PARAMS", "2", "v", q)
        check(r[1] == "p:1", "hybrid RRF puts the doc strong in both lists first (%s)" % r)
        check(r[0] >= 3, "hybrid unions both lists")
        r = c.call("FT.HYBRID", "docs", "SEARCH", "@cat:{food}", "VSIM", "@vec", "$v", "COMBINE", "LINEAR",
                   "ALPHA", "0", "BETA", "1", "NOCONTENT", "PARAMS", "2", "v", q)
        check(r[1] == "p:1", "LINEAR with BETA=1 follows the vector leg (%s)" % r)

        r = c.call("FT.AGGREGATE", "docs", "*", "GROUPBY", "1", "@cat",
                   "REDUCE", "COUNT", "0", "AS", "n",
                   "REDUCE", "SUM", "1", "@price", "AS", "total",
                   "REDUCE", "AVG", "1", "@price", "AS", "avg",
                   "REDUCE", "MIN", "1", "@price", "AS", "lo",
                   "REDUCE", "MAX", "1", "@price", "AS", "hi",
                   "REDUCE", "COUNT_DISTINCT", "1", "@title", "AS", "titles",
                   "REDUCE", "TOLIST", "1", "@title", "AS", "names",
                   "SORTBY", "2", "@total", "DESC")
        check(r[0] == 3, "three groups")
        rows = [dict(zip(x[0::2], x[1::2])) for x in r[1:]]
        check([x["cat"] for x in rows] == ["book", "food", "db"], "SORTBY @total DESC (%s)" % [x["cat"] for x in rows])
        book = rows[0]
        check(book["n"] == "2" and book["total"] == "50" and book["avg"] == "25" and book["lo"] == "20" and book["hi"] == "30",
              "reducers COUNT/SUM/AVG/MIN/MAX (%s)" % book)
        check(book["titles"] == "2" and sorted(book["names"]) == ["Database internals", "Rust for beginners"], "COUNT_DISTINCT / TOLIST")
        r = c.call("FT.AGGREGATE", "docs", "@price:[6 25]", "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "n")
        check(r[1:] == [["n", "3"]], "filtered global aggregate (%s)" % r)
        r = c.call("FT.AGGREGATE", "docs", "*", "LOAD", "1", "@price", "SORTBY", "2", "@price", "ASC", "LIMIT", "0", "2")
        check([dict(zip(x[0::2], x[1::2]))["price"] for x in r[1:]] == ["5", "7"], "LOAD + SORTBY + LIMIT")
        r = c.call("FT.AGGREGATE", "docs", "*", "LOAD", "1", "@price", "WITHCURSOR", "COUNT", "2")
        page, cursor = r
        check(len(page) == 3 and cursor != 0, "cursor first page")
        seen = len(page) - 1
        while cursor:
            page, cursor = c.call("FT.CURSOR", "READ", "docs", str(cursor), "COUNT", "2")
            seen += len(page) - 1
        check(seen == 5, "cursor walks every row (%d)" % seen)
        print("all text/hybrid/aggregate checks passed")
    finally:
        srv.terminate()
        srv.wait()


if __name__ == "__main__":
    main()
