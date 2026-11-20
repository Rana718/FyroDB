"""Concurrent binary string, pipeline, and RDB restart regressions."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time

from client import Client


BOUNDARIES = [(5, 7), (23, 24), (31, 32), (255, 256), (1023, 1024),
              (4095, 4096), (10239, 10240), (65535, 65536),
              (262143, 262144), (1048575, 1048576)]


def connect(port):
    return Client(port=port, binary=True, timeout=10)


@contextmanager
def server(binary, snapshot):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    env = dict(os.environ, FYRODB_PORT=str(port), FYRODB_BIND="127.0.0.1",
               FYRODB_RDB_PATH=str(snapshot), FYRODB_RDB_INTERVAL="0")
    env.setdefault("FYRODB_WORKERS", "6")
    process = subprocess.Popen([binary], env=env)
    try:
        deadline = time.monotonic() + 10
        while process.poll() is None:
            try:
                with connect(port) as c:
                    assert c.call("PING") == "PONG"
                break
            except ConnectionRefusedError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("server did not start")
                time.sleep(.05)
        else:
            raise RuntimeError(f"server exited during startup: {process.returncode}")
        yield port
        assert process.poll() is None, "server exited during test"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def pattern(prefix, size):
    return (prefix * ((size + len(prefix) - 1) // len(prefix)))[:size]


def describe(value):
    if isinstance(value, bytes):
        return f"{len(value)} bytes, sha256={hashlib.sha256(value).hexdigest()}"
    return repr(value)


def concurrent_values(port, small, large):
    values = (pattern(b"A\x00\xff\r\n", small), pattern(b"B\xfe\x00\n\r", large))
    key = "integrity:hot"
    with connect(port) as c:
        assert c.call("SET", key, values[0]) == "OK"
        assert c.call("SET", "marker", "x") == "OK"

    ready = threading.Barrier(9, timeout=10)
    stop = threading.Event()

    def worker(i):
        with connect(port) as c:
            ready.wait()
            deadline = time.monotonic() + 3
            count = 0
            try:
                while not stop.is_set() and time.monotonic() < deadline:
                    if i < 4:
                        assert c.call("SET", key, values[(i + count) % 2]) == "OK"
                    elif i == 8:
                        assert c.call("SAVE") == "OK"
                        stop.wait(.1)
                    else:
                        mode = count % 3
                        if mode == 0:
                            reply = c.call("GET", key)
                        else:
                            keys = (key, key) if mode == 1 else (key, "marker", key)
                            items = c.call("MGET", *keys)
                            assert isinstance(items, list) and len(items) == len(keys), "invalid MGET array"
                            if mode == 2:
                                assert items[1] == b"x", "MGET changed key order"
                            reply = items[0]
                            assert items[-1] == reply, f"MGET versions differ: {describe(reply)}, {describe(items[-1])}"
                        assert reply in values, f"unexpected hot-key value: {describe(reply)}"
                    count += 1
            except BaseException:
                stop.set()
                raise
            return count

    with ThreadPoolExecutor(max_workers=9) as pool:
        counts = list(pool.map(worker, range(9)))
    assert min(counts[:4]) >= 2 and min(counts[4:8]) >= 3 and counts[8] > 0, counts
    print(f"ok   concurrent SET/GET/MGET/SAVE at {small}/{large} bytes", flush=True)


def point_read_invalidation(port):
    key = "integrity:cached"
    with connect(port) as reader, connect(port) as writer:
        def expect(value):
            for _ in range(4):
                actual = reader.call("GET", key)
                assert actual == value, f"point GET expected {describe(value)}, got {describe(actual)}"

        assert writer.call("SET", key, b"old") == "OK"
        expect(b"old")
        for i in range(32):
            writer.send("SET", key, bytes([0xff, i]))
        assert [writer.read() for _ in range(32)] == ["OK"] * 32
        expect(bytes([0xff, 31]))
        assert writer.call("DEL", key) == 1
        expect(None)
        assert writer.call("SET", key, "new") == "OK"
        expect(b"new")
        assert writer.call("PEXPIRE", key, 100) == 1
        time.sleep(.15)
        expect(None)
        assert writer.call("SET", key, "again") == "OK"
        expect(b"again")
        assert writer.call("FLUSHALL") == "OK"
        expect(None)
    print("ok   point GET observed overwrite, delete, expiry, and flush", flush=True)


def increments(port):
    ready = threading.Barrier(8, timeout=10)
    with connect(port) as c:
        assert c.call("SET", "counter", 0) == "OK"

    def writer(_):
        replies = []
        with connect(port) as c:
            ready.wait()
            for _ in range(50):
                for _ in range(40):
                    c.send("INCR", "counter")
                replies.extend(c.read() for _ in range(40))
        return replies

    with ThreadPoolExecutor(max_workers=8) as pool:
        replies = [n for batch in pool.map(writer, range(8)) for n in batch]
    assert sorted(replies) == list(range(1, 16001)), "missing or duplicated INCR reply"
    with connect(port) as c:
        assert c.call("GET", "counter") == b"16000"
    print("ok   all 16,000 pipelined increments accounted for", flush=True)


def connection_burst(port, count):
    with ExitStack() as stack:
        clients = []
        for _ in range(count):
            c = stack.enter_context(connect(port))
            c.send("PING")
            clients.append(c)
        for i, c in enumerate(clients):
            assert c.read() == "PONG", i
            c.send("SET", f"burst:{i}", i)
            c.send("GET", f"burst:{i}")
        for i, c in enumerate(clients):
            assert c.read() == "OK" and c.read() == str(i).encode(), i
    print(f"ok   {count} simultaneous connections and ordered pipeline replies", flush=True)


def main(binary):
    persisted = {f"bytes:{size}": pattern(b"\x00\xffABC\r\n", size)
                 for size in (5, 24, 256, 65536, 1048576)}
    sets = {
        "set": [b"a", b"b"],
        "set:narrow": [b"-2147483648", b"0", b"2147483647"],
        "set:wide": [b"0", b"9223372036854775807", b"-9223372036854775808"],
        "set:mixed": [str(n).encode() for n in range(140)] + [b"01", b"-0"],
    }
    with tempfile.TemporaryDirectory(prefix="fyrodb-integrity-") as work:
        snapshot = Path(work) / "integrity.rdb"
        with server(binary, snapshot) as port:
            for small, large in BOUNDARIES:
                concurrent_values(port, small, large)
            point_read_invalidation(port)
            increments(port)
            for count in (100, 256):
                connection_burst(port, count)
            with connect(port) as c:
                assert c.call("FLUSHALL") == "OK"
                for key, value in persisted.items():
                    assert c.call("SET", key, value) == "OK"
                assert c.call("HSET", "hash", "field", b"\x00\xffvalue") == 1
                assert c.call("RPUSH", "list", "first", "second") == 2
                for key, members in sets.items():
                    assert c.call("SADD", key, *members, members[0]) == len(members), key
                assert c.call("ZADD", "zset", "1.25", "member") == 1
                assert c.call("SET", "ttl", "ok", "PX", 60000) == "OK"
                assert c.call("SAVE") == "OK" and snapshot.stat().st_size > 0
        with server(binary, snapshot) as port, connect(port) as c:
            for key, value in persisted.items():
                assert c.call("GET", key) == value, f"restart changed {key}"
            assert c.call("HGET", "hash", "field") == b"\x00\xffvalue"
            assert c.call("LRANGE", "list", 0, -1) == [b"first", b"second"]
            for key, members in sets.items():
                assert sorted(c.call("SMEMBERS", key)) == sorted(members), key
            assert c.call("ZSCORE", "zset", "member") == b"1.25"
            assert c.call("GET", "ttl") == b"ok" and 0 < c.call("PTTL", "ttl") <= 60000
        print("ok   binary values, collection types, and TTL survived restart", flush=True)


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()))
