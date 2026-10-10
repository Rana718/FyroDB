#!/usr/bin/env python3
"""End-to-end checks for streams and consumer groups against a running server.

    FYRODB_PORT=8000 python3 tests/e2e/python/stream_e2e.py
"""
import os
import socket
import threading
import time

PORT = int(os.environ.get("FYRODB_PORT", "8000"))


from client import Client, check


def err(v):
    return isinstance(v, Exception)


def main():
    c = Client()
    c.call("FLUSHALL")

    check(c.call("XADD", "s", "1-1", "a", "1") == "1-1", "XADD explicit id")
    check(err(c.call("XADD", "s", "1-1", "a", "1")), "XADD rejects equal id")
    check(err(c.call("XADD", "s", "0-0", "a", "1")), "XADD rejects 0-0 on a stream")
    check(c.call("XADD", "s", "1-*", "a", "2") == "1-2", "XADD ms-* picks next seq")
    for i in range(2, 300):
        c.call("XADD", "s", "%d-0" % i, "n", str(i))
    check(c.call("XLEN", "s") == 300, "XLEN after 300 adds")
    r = c.call("XRANGE", "s", "(1-2", "3", "COUNT", "2")
    check([e[0] for e in r] == ["2-0", "3-0"], "XRANGE exclusive start and COUNT")
    r = c.call("XREVRANGE", "s", "+", "-", "COUNT", "1")
    check(r[0][0] == "299-0" and r[0][1] == ["n", "299"], "XREVRANGE newest entry")
    r = c.call("XREAD", "COUNT", "2", "STREAMS", "s", "5-0")
    check([e[0] for e in r[0][1]] == ["6-0", "7-0"], "XREAD start id is exclusive")
    check(c.call("XREAD", "STREAMS", "s", "$") is None, "XREAD $ without BLOCK is nil")
    check(c.call("XDEL", "s", "3-0", "3-0", "999-0") == 1, "XDEL counts only real deletes")
    check(c.call("XTRIM", "s", "MAXLEN", "=", "250") == 49, "XTRIM exact MAXLEN")
    check(c.call("XLEN", "s") == 250, "length after exact trim")
    n = c.call("XTRIM", "s", "MAXLEN", "~", "10")
    check(n > 0 and c.call("XLEN", "s") >= 10, "XTRIM ~ drops whole nodes (%d)" % n)
    c.call("XTRIM", "s", "MINID", "280")
    check(c.call("XRANGE", "s", "-", "+", "COUNT", "1")[0][0] == "280-0", "XTRIM MINID")

    waiter = Client()
    got = {}

    def block():
        t0 = time.time()
        got["v"] = waiter.call("XREAD", "BLOCK", "3000", "STREAMS", "live", "$")
        got["dt"] = time.time() - t0

    th = threading.Thread(target=block)
    th.start()
    time.sleep(0.2)
    check("v" not in got, "XREAD BLOCK is waiting")
    nid = c.call("XADD", "live", "*", "k", "v")
    th.join(2)
    check(got.get("v") == [["live", [[nid, ["k", "v"]]]]], "XREAD BLOCK woke with the new entry")
    check(got["dt"] < 1.0, "XREAD wake latency (%.3fs)" % got["dt"])
    t0 = time.time()
    check(c.call("XREAD", "BLOCK", "200", "STREAMS", "live", "$") is None, "XREAD BLOCK timeout is nil")
    check(0.15 < time.time() - t0 < 1.0, "XREAD BLOCK honoured its timeout")

    c.call("DEL", "q")
    for i in range(1, 11):
        c.call("XADD", "q", "%d-0" % i, "job", str(i))
    check(c.call("XGROUP", "CREATE", "q", "g", "0") == "OK", "XGROUP CREATE")
    r = c.call("XGROUP", "CREATE", "q", "g", "0")
    check(err(r) and str(r).startswith("BUSYGROUP"), "duplicate group is BUSYGROUP")
    r = c.call("XGROUP", "CREATE", "nokey", "g", "0")
    check(err(r) and str(r).startswith("ERR"), "XGROUP CREATE on a missing key without MKSTREAM errors")
    r = c.call("XREADGROUP", "GROUP", "g", "alice", "COUNT", "4", "STREAMS", "q", ">")
    check([e[0] for e in r[0][1]] == ["1-0", "2-0", "3-0", "4-0"], "XREADGROUP > delivers new entries")
    r = c.call("XREADGROUP", "GROUP", "g", "bob", "STREAMS", "q", ">")
    check(len(r[0][1]) == 6, "second consumer gets the rest")
    check(c.call("XREADGROUP", "GROUP", "g", "bob", "STREAMS", "q", ">") is None, "nothing new is nil")
    r = c.call("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "q", "0")
    check(len(r[0][1]) == 4, "history read returns the consumer PEL")
    s = c.call("XPENDING", "q", "g")
    check(s[0] == 10 and s[1] == "1-0" and s[2] == "10-0" and s[3] == [["alice", "4"], ["bob", "6"]], "XPENDING summary")
    check(c.call("XACK", "q", "g", "1-0", "1-0", "77-0") == 1, "XACK counts unique pending ids")
    rows = c.call("XPENDING", "q", "g", "-", "+", "10", "alice")
    check([row[0] for row in rows] == ["2-0", "3-0", "4-0"] and rows[0][3] == 1, "XPENDING extended filtered by consumer")
    r = c.call("XCLAIM", "q", "g", "carol", "0", "2-0", "JUSTID")
    check(r == ["2-0"], "XCLAIM JUSTID")
    rows = c.call("XPENDING", "q", "g", "2-0", "2-0", "1")
    check(rows[0][1] == "carol", "claimed entry now owned by carol")
    c.call("XDEL", "q", "3-0")
    r = c.call("XAUTOCLAIM", "q", "g", "dave", "0", "0", "COUNT", "2")
    check(r[2] == ["3-0"], "XAUTOCLAIM reports deleted ids")
    check([e[0] for e in r[1]] == ["2-0", "4-0"], "XAUTOCLAIM claims idle entries")
    info = c.call("XINFO", "GROUPS", "q")
    check(info[0][1] == "g" and info[0][5] == 8, "XINFO GROUPS pending count")
    cons = c.call("XINFO", "CONSUMERS", "q", "g")
    check(sorted(x[1] for x in cons) == ["alice", "bob", "carol", "dave"], "XINFO CONSUMERS lists consumers")
    check(c.call("XGROUP", "DELCONSUMER", "q", "g", "bob") == 6, "DELCONSUMER returns dropped pending")
    r = c.call("XREADGROUP", "GROUP", "missing", "x", "STREAMS", "q", ">")
    check(err(r) and str(r).startswith("NOGROUP"), "unknown group is NOGROUP")

    gw = Client()
    got = {}
    th = threading.Thread(target=lambda: got.setdefault("v", gw.call("XREADGROUP", "GROUP", "g", "eve", "BLOCK", "3000", "STREAMS", "q", ">")))
    th.start()
    time.sleep(0.2)
    c.call("XADD", "q", "50-0", "job", "late")
    th.join(2)
    check(got.get("v") == [["q", [["50-0", ["job", "late"]]]]], "XREADGROUP BLOCK woke on XADD")
    check(c.call("XPENDING", "q", "g", "50-0", "50-0", "1")[0][1] == "eve", "blocked read added to PEL")

    si = c.call("XINFO", "STREAM", "q")
    d = dict(zip(si[0::2], si[1::2]))
    check(d["length"] == 10 and d["groups"] == 1 and d["last-generated-id"] == "50-0", "XINFO STREAM")

    check(c.call("SAVE") == "OK", "SAVE with streams and groups")
    print("all stream checks passed")


if __name__ == "__main__":
    main()
