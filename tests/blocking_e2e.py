#!/usr/bin/env python3
"""End-to-end checks for blocking commands against a running server.

    FYRODB_PORT=8000 python3 tests/blocking_e2e.py
"""
import os
import socket
import threading
import time

PORT = int(os.environ.get("FYRODB_PORT", "8000"))


class Client:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT))
        self.f = self.s.makefile("rb")

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)

    def read(self):
        line = self.f.readline()
        t, body = line[:1], line[1:-2]
        if t == b"*":
            n = int(body)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"$":
            n = int(body)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            return d[:-2].decode()
        if t == b":":
            return int(body)
        if t == b"-":
            return Exception(body.decode())
        return body.decode()

    def call(self, *args):
        self.send(*args)
        return self.read()


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL: " + msg)
    print("ok  ", msg)


def main():
    c = Client()
    c.call("FLUSHALL")

    # Immediate serve when data exists.
    c.call("RPUSH", "q", "a", "b")
    check(c.call("BLPOP", "q", "0") == ["q", "a"], "BLPOP serves existing element")
    check(c.call("BRPOP", "q", "0") == ["q", "b"], "BRPOP serves existing element")

    # Blocks, then a push from another client wakes it.
    waiter = Client()
    got = {}

    def block():
        t0 = time.time()
        got["v"] = waiter.call("BLPOP", "empty1", "empty2", "5")
        got["dt"] = time.time() - t0

    th = threading.Thread(target=block)
    th.start()
    time.sleep(0.2)
    check("v" not in got, "BLPOP is still blocked after 200ms")
    check(c.call("LPUSH", "empty2", "x") == 1, "LPUSH to the watched key")
    th.join(2)
    check(got.get("v") == ["empty2", "x"], "blocked BLPOP woke with the pushed value")
    check(got["dt"] < 1.0, "wake latency well under the timeout (%.3fs)" % got["dt"])
    check(c.call("LLEN", "empty2") == 0, "pushed element was consumed exactly once")

    # Timeout returns nil after roughly the timeout.
    t0 = time.time()
    r = c.call("BLPOP", "never", "0.3")
    dt = time.time() - t0
    check(r is None, "BLPOP timeout returns nil")
    check(0.25 <= dt < 1.0, "timeout honoured (%.3fs)" % dt)

    # Pipelined commands behind a blocked one keep their order.
    p = Client()
    p.send("BLPOP", "pipe", "2")
    p.send("SET", "after", "1")
    p.send("GET", "after")
    time.sleep(0.2)
    check(c.call("GET", "after") is None, "commands behind a blocked BLPOP do not run early")
    c.call("RPUSH", "pipe", "v")
    check(p.read() == ["pipe", "v"], "pipelined BLPOP reply first")
    check(p.read() == "OK", "then the SET")
    check(p.read() == "1", "then the GET")

    # BLMOVE / BRPOPLPUSH.
    m = Client()
    res = {}
    th = threading.Thread(target=lambda: res.setdefault("v", m.call("BLMOVE", "src", "dst", "LEFT", "RIGHT", "5")))
    th.start()
    time.sleep(0.1)
    c.call("RPUSH", "src", "item")
    th.join(2)
    check(res.get("v") == "item", "BLMOVE woke and moved the element")
    check(c.call("LRANGE", "dst", "0", "-1") == ["item"], "element landed in dst")
    c.call("RPUSH", "src2", "z")
    check(c.call("BRPOPLPUSH", "src2", "dst2", "1") == "z", "BRPOPLPUSH immediate")
    check(m.call("BLMOVE", "none", "dst", "LEFT", "RIGHT", "0.1") is None, "BLMOVE timeout is nil")

    # BZPOPMIN wakes on ZADD.
    z = Client()
    res = {}
    th = threading.Thread(target=lambda: res.setdefault("v", z.call("BZPOPMIN", "zs", "5")))
    th.start()
    time.sleep(0.1)
    c.call("ZADD", "zs", "3", "m3", "1", "m1")
    th.join(2)
    check(res.get("v") == ["zs", "m1", "1"], "BZPOPMIN woke with the lowest member")

    # Errors.
    c.call("SET", "str", "v")
    check(isinstance(c.call("BLPOP", "str", "1"), Exception), "WRONGTYPE for a string key")
    check(isinstance(c.call("BLPOP", "q", "-1"), Exception), "negative timeout rejected")
    check(isinstance(c.call("BLPOP", "q", "abc"), Exception), "non-numeric timeout rejected")

    # A client that disconnects while blocked leaves no waiter behind.
    d = Client()
    d.send("BLPOP", "gone", "0")
    time.sleep(0.1)
    d.f.close()
    d.s.close()
    time.sleep(0.1)
    check(c.call("RPUSH", "gone", "kept") == 1, "push after a blocked client left")
    check(c.call("LRANGE", "gone", "0", "-1") == ["kept"], "nobody consumed it")

    # Many waiters across workers: each push serves exactly one.
    n = 16
    clients = [Client() for _ in range(n)]
    results = []
    lock = threading.Lock()

    def one(cl):
        v = cl.call("BLPOP", "fan", "5")
        with lock:
            results.append(v)

    threads = [threading.Thread(target=one, args=(cl,)) for cl in clients]
    for t in threads:
        t.start()
    time.sleep(0.3)
    for i in range(n):
        c.call("RPUSH", "fan", str(i))
    for t in threads:
        t.join(3)
    vals = sorted(int(r[1]) for r in results if r)
    check(vals == list(range(n)), "16 blocked clients each got exactly one element")
    # Drained lists read as absent and LRANGE replies stay well-formed.
    c.call("RPUSH", "dr", "1")
    c.call("LPOP", "dr")
    check(c.call("EXISTS", "dr") == 0, "drained list does not exist")
    check(c.call("TYPE", "dr") == "none", "drained list has type none")
    check(c.call("LRANGE", "dr", "0", "-1") == [], "LRANGE on a drained list is empty")
    c.call("RPUSH", "r3", "a", "b", "c")
    check(c.call("LRANGE", "r3", "2", "1") == [], "LRANGE start > stop replies empty")
    print("all blocking checks passed")


if __name__ == "__main__":
    main()
