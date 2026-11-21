"""Exercise slot routing, migration and replication over real loopback connections."""

from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from client import Client


@contextmanager
def node(binary, directory, port, cluster_env=None):
    directory.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("FYRODB_")}
    env.update(TMPDIR=str(directory), FYRODB_BIND="127.0.0.1", FYRODB_PORT=str(port), FYRODB_WORKERS="1",
               FYRODB_SHARDS="4", FYRODB_RDB_PATH=str(directory / "dump.rdb"),
               FYRODB_RDB_INTERVAL="0")
    env.update(cluster_env or {})
    with (directory / "server.log").open("w+") as log:
        proc = subprocess.Popen([binary], cwd=directory, env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            while proc.poll() is None and time.monotonic() < deadline:
                try:
                    with Client(port=port, timeout=1) as client:
                        if client.call("PING") == "PONG":
                            break
                except OSError:
                    time.sleep(.05)
            else:
                raise RuntimeError(f"{directory.name} failed to start")
            yield
            assert proc.poll() is None, f"{directory.name} exited during the test"
        except BaseException:
            log.seek(0)
            print(f"{directory.name} server log:\n{log.read()[-4000:]}", file=sys.stderr)
            raise
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def expect_error(reply, message):
    assert isinstance(reply, Exception) and str(reply) == message, (reply, message)


def owner(client, slot):
    matches = [primary[2] for lo, hi, primary, *_ in client.call("CLUSTER", "SLOTS")
               if lo <= slot <= hi]
    assert len(matches) == 1, (slot, matches)
    return matches[0]


def check_routing(a, b, ports):
    left, right = "value{bar}", "value{foo}"
    slot = b.call("CLUSTER", "KEYSLOT", right)
    moved = f"MOVED {slot} 127.0.0.1:{ports[1]}"
    payload = bytes(range(256)) * 256
    assert a.call("SET", left, payload) == "OK"
    assert b.call("SET", right, b"original") == "OK"
    cross_slot = "CROSSSLOT Keys in request don't hash to the same slot"
    expect_error(a.call("MSET", left, "bad", right, "bad"), cross_slot)
    assert b.call("GET", right) == b"original"

    # Cross the 64-command run boundary with redirects on either side of it.
    commands = [("GET", right if i in (63, 65) else left) for i in range(67)]
    a.send_pipeline([*commands, ("PING",)])
    for i in range(67):
        if i in (63, 65):
            expect_error(a.read(), moved)
        else:
            assert a.read() == payload, f"corrupt or misplaced large reply {i}"
    assert a.read() == "PONG"
    print("ok   routed binary replies, MOVED ordering and all-or-nothing CROSSSLOT rejection")


def check_overlapping_reads(writer, port):
    key = "changing{bar}"
    old = bytes(range(256)) * 256
    new = bytes(reversed(range(256))) * 257
    with Client(port=port, binary=True, timeout=10) as reader:
        for before, after in [(old, new), (new, old)]:
            assert writer.call("SET", key, before) == "OK"
            reader.send_pipeline([("GET", key)] * 65)
            assert writer.call("SET", key, after) == "OK"
            reader.send("GET", key)
            seen_after = False
            for i in range(65):
                reply = reader.read()
                assert reply == after or (not seen_after and reply == before), f"torn or stale reply {i}"
                seen_after |= reply == after
            assert reader.read() == after, "GET issued after acknowledged SET returned the old value"
    print("ok   pending large replies survive binary overwrites in both size directions")


def check_migration(a, b, ports):
    key = "import{foo}"
    slot = b.call("CLUSTER", "KEYSLOT", key)
    moved = f"MOVED {slot} 127.0.0.1:{ports[1]}"
    assert owner(a, slot) == owner(b, slot) == b"b"
    assert a.call("CLUSTER", "SETSLOT", slot, "IMPORTING", "b") == "OK"
    assert b.call("CLUSTER", "SETSLOT", slot, "MIGRATING", "a") == "OK"
    expect_error(b.call("GET", key), f"ASK {slot} 127.0.0.1:{ports[0]}")
    expect_error(a.call("SET", key, "denied"), moved)
    a.send("ASKING")
    a.send("SET", key, "imported")
    a.send("SET", key, "denied")
    assert a.read() == "OK" and a.read() == "OK"
    expect_error(a.read(), moved)
    with Client(port=ports[0], binary=True, timeout=5) as other:
        assert a.call("ASKING") == "OK"
        expect_error(other.call("GET", key), moved)
        assert a.call("GET", key) == b"imported"
        expect_error(a.call("GET", key), moved)

    for client in (b, a):
        assert client.call("CLUSTER", "SETSLOT", slot, "NODE", "a") == "OK"
    assert a.call("CLUSTER", "SETSLOT", slot, "STABLE") == "OK"
    for client in (a, b):
        assert [owner(client, k) for k in (slot-1, slot, slot+1)] == [b"b", b"a", b"b"]
    expect_error(b.call("GET", key), f"MOVED {slot} 127.0.0.1:{ports[0]}")
    assert a.call("GET", key) == b"imported"
    print("ok   ASK handoff, connection-local one-shot ASKING and live owner refresh")


def info_number(client, field):
    info = client.call("INFO").decode()
    return int(dict(line.split(":", 1) for line in info.splitlines() if ":" in line)[field])


def check_replication(a, replica):
    values = {f"row:{i}{{bar}}": bytes([i, 0, 255, 13, 10]) for i in range(130)}
    expect_error(replica.call("SET", "row:0{bar}", "bad"),
                 "READONLY You can't write against a read only replica.")
    a.send_pipeline(("SET", key, value) for key, value in values.items())
    assert all(a.read() == "OK" for _ in values)
    values["row:0{bar}"] = b"replaced\x00\xff"
    assert a.call("SET", "row:0{bar}", values["row:0{bar}"]) == "OK"
    assert a.call("DEL", "row:1{bar}") == 1
    del values["row:1{bar}"]
    target = info_number(a, "cluster_replication_next_offset") - 1
    assert target >= 132
    deadline = time.monotonic() + 15
    while info_number(replica, "cluster_replica_applied_offset") < target:
        observed_owner = owner(replica, 5061)
        assert observed_owner == b"a", f"replica changed owner to {observed_owner!r} with the primary alive"
        if time.monotonic() >= deadline:
            applied = info_number(replica, "cluster_replica_applied_offset")
            raise AssertionError(f"replica still at {applied}, expected at least {target}")
        time.sleep(.05)
    assert replica.call("SAVE") == "OK"
    return values


def check_healthy_startup(a, b, replica):
    slot = a.call("CLUSTER", "KEYSLOT", "startup{bar}")
    assert a.call("SET", "startup{bar}", "alive") == "OK"
    target = info_number(a, "cluster_replication_next_offset") - 1
    deadline = time.monotonic() + 15
    while True:
        for client in (a, b, replica):
            observed = owner(client, slot)
            assert observed == b"a", f"unexpected primary {observed!r} during healthy startup"
        if (all(info_number(c, "cluster_peer_healthy") == 2 for c in (a, b, replica))
                and info_number(replica, "cluster_replica_applied_offset") >= target):
            break
        if time.monotonic() >= deadline:
            raise AssertionError("cluster failed to establish healthy links and replicate the startup write")
        time.sleep(.05)
    print("ok   three healthy nodes converge without promoting the replica at startup")


def check_primary_loss(b, replica, stop_primary, timeout_ms):
    slot = b.call("CLUSTER", "KEYSLOT", "startup{bar}")
    started = time.monotonic()
    stop_primary()
    deadline = started + timeout_ms / 1000 + 15
    while True:
        owners = [owner(client, slot) for client in (b, replica)]
        elapsed = time.monotonic() - started
        if b"r" in owners:
            assert elapsed >= timeout_ms / 1000, f"primary replaced after only {elapsed:.3f}s"
        if owners == [b"r", b"r"]:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(f"surviving nodes did not reassign the failed primary's slots: {owners}")
        time.sleep(.05)
    assert replica.call("GET", "startup{bar}") == b"alive"
    written = replica.call("SET", "startup{bar}", "after failover")
    assert written == "OK", f"promoted primary rejected a write: {written!r}"
    assert replica.call("GET", "startup{bar}") == b"after failover"
    assert owner(b, 16383) == owner(replica, 16383) == b"b"
    print("ok   sustained primary loss triggers quorum slot reassignment after the suspicion timeout")


@contextmanager
def cluster(binary, root, members, **settings):
    root.mkdir()
    with ExitStack() as stack:
        count = len(members)
        sockets = [stack.enter_context(socket.socket()) for _ in range(2 * count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in sockets]
        config = "".join(f"{name} 127.0.0.1:{ports[i]}@127.0.0.1:{ports[i+count]} "
                         f"{role} 1 0 connected {slots}\n"
                         for i, (name, role, slots) in enumerate(members))
        stop = []
        for i, (name, _, _) in enumerate(members):
            conf = root / f"{name}.conf"
            conf.write_text(config)
            sockets[i].close()
            sockets[i+count].close()
            lifetime = stack.enter_context(ExitStack())
            stop.append(lifetime.close)
            lifetime.enter_context(node(binary, root / name, ports[i], {
                **settings,
                "FYRODB_CLUSTER_ENABLED": "true", "FYRODB_NODE_ID": name,
                "FYRODB_WORKERS": "2" if name == "a" else "1",
                "FYRODB_CLUSTER_PORT": str(ports[i+count]), "FYRODB_CLUSTER_CONFIG_FILE": str(conf),
            }))
        clients = [stack.enter_context(Client(port=p, binary=True, timeout=10))
                   for p in ports[:count]]
        for client, (name, _, _) in zip(clients, members):
            assert client.call("CLUSTER", "MYID") == name.encode()
        yield clients, ports[:count], stop


def main(binary):
    primaries = [("a", "master -", "0-8191"), ("b", "master -", "8192-16383")]
    with tempfile.TemporaryDirectory(prefix="fyro-cluster-e2e-") as tmp:
        root = Path(tmp)
        with cluster(binary, root / "routing", primaries) as ((a, b), ports, _):
            check_routing(a, b, ports)
            check_overlapping_reads(a, ports[0])
            assert b.call("DEL", "value{foo}") == 1
            check_migration(a, b, ports)

        pair = [("a", "master -", "0-16383"), ("r", "slave a", "")]
        with cluster(binary, root / "replication", pair) as ((a, replica), _, _stops):
            values = check_replication(a, replica)

        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        snapshot = root / "replication" / "r" / "dump.rdb"
        assert snapshot.is_file()
        with node(binary, root / "restored", port, {"FYRODB_RDB_PATH": str(snapshot)}), \
                Client(port=port, binary=True, timeout=5) as client:
            assert client.call("DBSIZE") == len(values)
            assert client.call("GET", "row:1{bar}") is None
            assert all(client.call("GET", key) == value for key, value in values.items())
        print("ok   replica rejects writes and restores every acknowledged binary row and deletion")

        timeout_ms = 6000
        with cluster(binary, root / "startup", [*primaries, ("r", "slave a", "")],
                     FYRODB_CLUSTER_SUSPECT_MS=str(timeout_ms)) as (clients, _, stop):
            check_healthy_startup(*clients)
            check_primary_loss(clients[1], clients[2], stop[0], timeout_ms)


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()))
