import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import tempfile
import time

import client
from client import Client


def connect():
    c = Client()
    c.s.settimeout(5)
    return c


def close(c):
    c.f.close()
    c.s.close()


def functional():
    a, b = connect(), connect()
    try:
        assert a.call("FLUSHALL") == "OK"
        assert isinstance(a.call("EXEC"), Exception)
        assert isinstance(a.call("DISCARD"), Exception)
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "queued", "1") == "QUEUED"
        assert b.call("GET", "queued") is None
        assert a.call("INCR", "queued") == "QUEUED"
        a.send("EXEC")
        a.send("GET", "queued")
        a.send("PING")
        assert a.read() == ["OK", 2]
        assert a.read() == "2"
        assert a.read() == "PONG"
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "discarded", "1") == "QUEUED"
        assert a.call("DISCARD") == "OK"
        assert b.call("GET", "discarded") is None
        for bad in [("GET",), ("not-a-command",), ("MSET", "a")]:
            assert a.call("MULTI") == "OK"
            assert a.call("SET", "uncommitted", "1") == "QUEUED"
            assert isinstance(a.call(*bad), Exception)
            assert str(a.call("EXEC")).startswith("EXECABORT")
            assert b.call("GET", "uncommitted") is None
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "text", "not-a-number") == "QUEUED"
        assert a.call("INCR", "text") == "QUEUED"
        assert a.call("SET", "after-error", "ok") == "QUEUED"
        reply = a.call("EXEC")
        assert reply[0] == "OK" and isinstance(reply[1], Exception) and reply[2] == "OK"
        for mutation in [
            lambda: b.call("SET", "watched", "1"),
            lambda: (b.call("DEL", "watched"), b.call("SET", "watched", "1")),
            lambda: b.call("FLUSHALL"),
            lambda: b.call("EXPIRE", "watched", "60"),
        ]:
            assert b.call("SET", "watched", "1") == "OK"
            assert a.call("WATCH", "watched") == "OK"
            mutation()
            assert a.call("MULTI") == "OK"
            assert a.call("SET", "watch-output", "bad") == "QUEUED"
            assert a.call("EXEC") is None
        assert b.call("SET", "watched", "1", "PX", "50") == "OK"
        assert a.call("WATCH", "watched") == "OK"
        time.sleep(0.08)
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "expired-output", "bad") == "QUEUED"
        assert a.call("EXEC") is None
        for cancel in ["UNWATCH", "DISCARD"]:
            assert a.call("WATCH", "absent") == "OK"
            assert b.call("SET", "absent", "1") == "OK"
            if cancel == "DISCARD":
                assert a.call("MULTI") == "OK"
            assert a.call(cancel) == "OK"
            assert a.call("MULTI") == "OK"
            assert a.call("INCR", "absent") == "QUEUED"
            assert a.call("EXEC") == [2]
        assert a.call("WATCH", "stable") == "OK"
        assert b.call("SET", "unrelated", "1") == "OK"
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "stable", "1") == "QUEUED"
        assert a.call("EXEC") == ["OK"]
        for cancel in ["RESET", "QUIT"]:
            assert a.call("WATCH", "reset-key") == "OK"
            assert a.call("MULTI") == "OK"
            assert a.call("SET", "reset-output", "bad") == "QUEUED"
            assert a.call(cancel) == ("RESET" if cancel == "RESET" else "OK")
            assert isinstance(a.call("EXEC"), Exception)
            assert b.call("GET", "reset-output") is None
        assert a.call("MULTI") == "OK"
        assert a.call("BLPOP", "empty-transaction-list", "0") == "QUEUED"
        assert a.call("PING") == "QUEUED"
        assert a.call("EXEC") == [None, "PONG"]
        blocked = connect()
        try:
            blocked.send("BLPOP", "before-script", "0")
            blocked.send("EVAL", "return 123", "0")
            time.sleep(.03)
            assert b.call("RPUSH", "before-script", "item") == 1
            assert blocked.read() == ["before-script", "item"]
            assert blocked.read() == 123
        finally:
            close(blocked)
        assert a.call("FT.CREATE", "watch-index", "ON", "HASH", "PREFIX", "1", "indexed:", "SCHEMA", "value", "TEXT") == "OK"
        assert a.call("WATCH", "indexed:key") == "OK"
        assert b.call("FT.DROPINDEX", "watch-index") == "OK"
        assert b.call("HSET", "indexed:key", "value", "changed") == 1
        assert a.call("MULTI") == "OK"
        assert a.call("PING") == "QUEUED"
        assert a.call("EXEC") is None
        for subscribe, unsubscribe in [("SUBSCRIBE", "UNSUBSCRIBE"), ("PSUBSCRIBE", "PUNSUBSCRIBE")]:
            assert a.call("WATCH", "subscribed-watch") == "OK"
            assert a.call(subscribe, "channel") == [subscribe.lower(), "channel", 1]
            assert b.call("SET", "subscribed-watch", "changed") == "OK"
            assert a.call(unsubscribe) == [unsubscribe.lower(), "channel", 0]
            assert a.call("MULTI") == "OK"
            assert a.call("PING") == "QUEUED"
            assert a.call("EXEC") is None
        assert a.call("MULTI") == "OK"
        assert a.call("SET", "binary", b"\x00\xff\xfe") == "QUEUED"
        assert a.call("GET", "binary") == "QUEUED"
        a.send("EXEC")
        assert a.f.readline() == b"*2\r\n"
        assert a.f.readline() == b"+OK\r\n"
        assert a.f.readline() == b"$3\r\n"
        assert a.f.read(5) == b"\x00\xff\xfe\r\n"
        assert a.call("PING") == "PONG"
        for command in ["SAVE", "BGSAVE", "MULTI", "EXEC", "WATCH", "UNWATCH"]:
            assert isinstance(a.call("EVAL", "return redis.call('"+command+"')", "0"), Exception)
            reply = a.call("EVAL", "return redis.pcall('"+command+"').err", "0")
            assert "not allowed from scripts" in reply, (command, reply)
        assert isinstance(a.call("EVAL", "local =", "0"), Exception)
        assert a.call("PING") == "PONG"
        assert str(a.call("EVAL", "return redis.error_reply('first\\r\\nsecond')", "0")) == "ERR first  second"
        assert a.call("EVAL", "return redis.status_reply('first\\nsecond')", "0") == "first second"
        assert a.call("PING") == "PONG"
    finally:
        close(a)
        close(b)


TRANSFER = """local a=tonumber(redis.call('GET',KEYS[1]));
if a>0 then redis.call('SET',KEYS[1],a-1);
redis.call('SET',KEYS[2],tonumber(redis.call('GET',KEYS[2]))+1); end; return 1"""


def stress(mode, duration):
    c = connect()
    assert c.call("FLUSHALL") == "OK"
    keys = ["balance:%d" % i for i in range(64)]
    assert c.call("MSET", *[p for key in keys for p in (key, 1000)]) == "OK"
    assert c.call("SET", "counter", "0") == "OK"
    deadline = time.monotonic() + duration

    def worker(index):
        c = connect()
        rng = random.Random(index)
        commits = aborts = inconsistent = errors = 0
        examples = []
        try:
            while time.monotonic() < deadline:
                if index >= 12:
                    value = c.call("GET", "counter") if mode == "watch" else c.call("MGET", *keys)
                    if mode != "watch" and (not isinstance(value, list) or sum(map(int, value)) != 64000):
                        inconsistent += 1
                        if len(examples) < 3:
                            examples.append(dict(total=sum(map(int, value)) if isinstance(value, list) else None, reply=repr(value)))
                elif mode == "watch":
                    if c.call("WATCH", "counter") != "OK":
                        errors += 1
                        continue
                    value = int(c.call("GET", "counter"))
                    c.send("MULTI")
                    c.send("SET", "counter", value + 1)
                    c.send("EXEC")
                    replies = [c.read(), c.read(), c.read()]
                    if replies == ["OK", "QUEUED", ["OK"]]:
                        commits += 1
                    elif replies == ["OK", "QUEUED", None]:
                        aborts += 1
                    else:
                        errors += 1
                else:
                    a, b = rng.sample(keys, 2)
                    if mode == "lua":
                        if c.call("EVAL", TRANSFER, 2, a, b) != 1:
                            errors += 1
                    else:
                        for args in [("MULTI",), ("DECR", a), ("INCR", b), ("EXEC",)]:
                            c.send(*args)
                        replies = [c.read() for _ in range(4)]
                        if replies[:3] != ["OK", "QUEUED", "QUEUED"] or not isinstance(replies[3], list):
                            errors += 1
                    commits += 1
        except Exception:
            errors += 1
        finally:
            close(c)
        return commits, aborts, inconsistent, errors, examples

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        counts = list(pool.map(worker, range(16)))
    totals = [sum(row[i] for row in counts) for i in range(4)]
    final = int(c.call("GET", "counter")) if mode == "watch" else sum(map(int, c.call("MGET", *keys)))
    close(c)
    expected = totals[0] if mode == "watch" else 64000
    examples = [example for row in counts for example in row[4]][:5]
    return dict(mode=mode, seconds=duration, commits=totals[0], aborts=totals[1], inconsistent=totals[2], inconsistent_examples=examples, errors=totals[3], final=final, expected=expected,
                passed=totals[2] == 0 and totals[3] == 0 and final == expected)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--seconds", type=float, default=15)
    parser.add_argument("--stress-only", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fyrodb-isolation-") as directory:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            client.PORT = probe.getsockname()[1]
        env = dict(os.environ, FYRODB_PORT=str(client.PORT), FYRODB_WORKERS="12", FYRODB_BIND="127.0.0.1", FYRODB_RDB_INTERVAL="86400", FYRODB_RDB_PATH=directory+"/dump.rdb")
        with open(directory+"/server.log", "w+") as log:
            server = subprocess.Popen([str(Path(args.binary).resolve())], env=env, stdout=log, stderr=log)
            try:
                for _ in range(200):
                    try:
                        c = connect()
                        close(c)
                        break
                    except OSError:
                        if server.poll() is not None:
                            raise RuntimeError("server exited during startup")
                        time.sleep(.02)
                else:
                    raise RuntimeError("server startup timed out")
                if not args.stress_only:
                    functional()
                    print("Functional transaction checks passed", flush=True)
                results = [stress(mode, args.seconds) for mode in ("lua", "multi", "watch")]
                print(json.dumps(results, indent=2), flush=True)
                assert all(r["passed"] for r in results), "isolation checks failed"
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
                if server.returncode not in (0, -9):
                    log.seek(0)
                    print(log.read()[-4000:])


if __name__ == "__main__":
    main()
