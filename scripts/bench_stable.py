#!/usr/bin/env python3
"""Run pinned, alternating benchmarks and retain samples, variability, and README comparisons."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import socket
import statistics
import subprocess
import tempfile
import threading
import time


README = {
    "Pipelined GET": 53.38e6,
    "Pipelined SET": 39.62e6,
    "Mixed SET/GET (50/50)": 45.87e6,
    "INCR (atomic counters)": 108.56e6,
    "Pub/Sub delivery": 113.98e6,
    "Hot Key (1 key, contention)": 37.68e6,
}


def cpus(value):
    result = set()
    for part in value.split(","):
        ends = list(map(int, part.split("-")))
        result.update(range(ends[0], ends[-1] + 1))
    return sorted(result)


def default_cpus():
    allowed = sorted(os.sched_getaffinity(0))
    groups = {}
    for cpu in allowed:
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = tuple((topology / name).read_text().strip() for name in ("physical_package_id", "core_id"))
        groups.setdefault(key, []).append(cpu)
    physical = list(groups.values())
    split = max(1, len(physical) * 2 // 3)
    server = sorted(cpu for group in physical[:split] for cpu in group)
    client = sorted(cpu for group in physical[split:] for cpu in group) or allowed
    return server, client


def rss(pid):
    try:
        return int(next(line.split()[1] for line in Path(f"/proc/{pid}/status").read_text().splitlines() if line.startswith("VmRSS:")))
    except (OSError, StopIteration):
        return 0


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def system_cpu():
    fields = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
    return sum(fields), sum(fields) - fields[3] - fields[4]


def pressure():
    result = {}
    for name in ("cpu", "memory", "io"):
        path = Path(f"/proc/pressure/{name}")
        if path.exists():
            result[name] = float(path.read_text().splitlines()[0].split()[1].split("=")[1])
    return result


def wait_idle(args):
    deadline = time.monotonic() + args.wait_idle
    while True:
        before = system_cpu()
        time.sleep(.5)
        after = system_cpu()
        busy = 100 * (after[1] - before[1]) / max(1, after[0] - before[0])
        state = dict(cpu_busy_pct=busy, pressure_avg10=pressure(), load_average=list(os.getloadavg()))
        if args.wait_idle == 0 or (busy <= 15 and state["pressure_avg10"].get("memory", 0) <= 1):
            return state
        if time.monotonic() >= deadline:
            raise RuntimeError(f"host is contended; no stable sample recorded: {state}")
        print(f"waiting for idle host: {busy:.1f}% CPU busy", flush=True)


def ping(port):
    with socket.create_connection(("127.0.0.1", port), timeout=1) as stream:
        stream.sendall(b"*1\r\n$4\r\nPING\r\n")
        with stream.makefile("rb") as reply:
            if reply.readline() != b"+PONG\r\n":
                raise RuntimeError("server did not return PONG")


def environment():
    env = {k: v for k, v in os.environ.items() if not k.startswith("FYRODB_")}
    env.update(MIMALLOC_ALLOW_THP="0", MIMALLOC_PURGE_DELAY="0", MIMALLOC_PURGE_DECOMMITS="1", MIMALLOC_ARENA_RESERVE="0", MIMALLOC_ABANDONED_PAGE_PURGE="1", MIMALLOC_ARENA_EAGER_COMMIT="0")
    return env


def grouped(rows):
    result = {}
    for row in rows:
        item = result.setdefault(row["label"], {"ops": 0, "elapsed_ns": 0})
        item["ops"] += row["ops"]
        item["elapsed_ns"] += row["elapsed_ns"]
    for row in result.values():
        row["ops_per_sec"] = row["ops"] * 1e9 / row["elapsed_ns"]
    return result


def distribution(values):
    median = statistics.median(values)
    rng = random.Random(0)
    draws = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(3000))
    return dict(median=median, best=max(values), minimum=min(values), maximum=max(values), cv_pct=100 * statistics.stdev(values) / statistics.mean(values) if len(values) > 1 else 0,
                median_ci95=[draws[75], draws[2924]], samples=values)


def run_tcp(args, label, binary, mode, index, root):
    host_before = wait_idle(args)
    prefix = args.out / f"tcp-{label}-{mode}-{index}"
    env = environment()
    env.update(FYRODB_WORKERS=str(args.workers), FYRODB_WORKER_CORES=",".join(map(str, args.server_cpus)), FYRODB_BIND="127.0.0.1", FYRODB_RDB_INTERVAL="86400", GOMAXPROCS=str(len(args.client_cpus)), MIX_ONLY="mixed,incr,hotkey", MIX_REPEAT=str(args.inner))
    if mode == "hotkey":
        env.update(MIX_ONLY="hotkey", MIX_REPEAT="1")
    with tempfile.TemporaryDirectory(prefix="server-", dir=args.out) as work:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        env.update(FYRODB_PORT=str(port), FYRODB_RDB_PATH=str(Path(work) / "snapshot.rdb"))
        with open(str(prefix) + ".server.log", "w") as log:
            server = subprocess.Popen(["taskset", "-c", ",".join(map(str, args.server_cpus)), str(binary)], env=env, stdout=log, stderr=log, cwd=root)
            done = threading.Event()
            samples = []
            monitor = None
            try:
                for _ in range(200):
                    try:
                        ping(port)
                        break
                    except OSError:
                        if server.poll() is not None:
                            raise RuntimeError(f"server exited: {prefix}.server.log")
                        time.sleep(.02)
                else:
                    raise RuntimeError("server startup timed out")

                def sample():
                    while not done.wait(.02):
                        samples.append(rss(server.pid))

                monitor = threading.Thread(target=sample)
                monitor.start()
                cpu_start = cpu_seconds(server.pid)
                system_start = system_cpu()[1]
                child_start = resource.getrusage(resource.RUSAGE_CHILDREN)
                self_start = time.process_time()
                wall_start = time.monotonic()
                rows = []
                repeats = args.inner if mode in ("key", "pub") else 1
                for part in range(repeats):
                    output = Path(f"{prefix}-{part}.json")
                    command = ["taskset", "-c", ",".join(map(str, args.client_cpus)), str(args.tcp_bench), "-p", str(port), "-pid", str(server.pid), "-m", "mix" if mode == "hotkey" else mode,
                               "-clients", "100", "-ops", "100000", "-mix-ops", "20000", "-hotkey-ops", "100000", "-pub-subs", "50", "-pub-pubs", "10", "-pub-msgs", "20000", "-json", str(output)]
                    if mode == "hotkey":
                        command += ["-hotkey-duration", f"{args.hotkey_seconds}s"]
                    with open(f"{prefix}-{part}.log", "w") as bench_log:
                        subprocess.run(command, env=env, stdout=bench_log, stderr=bench_log, timeout=args.timeout, check=True)
                    report = json.loads(output.read_text())
                    rows.extend(report["throughput"])
                    if mode == "pub":
                        assert next(row for row in rows[-2:] if row["label"] == "Pub/Sub delivery")["ops"] == 10_000_000
                ping(port)
                if server.poll() is not None:
                    raise RuntimeError("server exited during workload")
                used_cpu = cpu_seconds(server.pid) - cpu_start
                child_end = resource.getrusage(resource.RUSAGE_CHILDREN)
                client_cpu = child_end.ru_utime + child_end.ru_stime - child_start.ru_utime - child_start.ru_stime
                busy_cpu = (system_cpu()[1] - system_start) / os.sysconf("SC_CLK_TCK")
                unattributed = max(0, busy_cpu - used_cpu - client_cpu - (time.process_time() - self_start)) / (time.monotonic() - wall_start)
                operations = sum(row["ops"] for row in rows)
                return dict(transport="tcp", side=label, mode=mode, round=index, rows=grouped(rows), peak_rss_kib=max(samples, default=rss(server.pid)), cpu_seconds=used_cpu, cpu_ns_per_operation=used_cpu * 1e9 / operations, host_before=host_before, pressure_after=pressure(), unattributed_cpu_cores=unattributed)
            finally:
                done.set()
                if monitor:
                    monitor.join()
                if server.poll() is None:
                    server.kill()
                server.wait(timeout=5)


def run_direct(args, mode, index):
    wait_idle(args)
    prefix = args.out / f"direct-{mode}-{index}"
    env = environment()
    env["FYRODB_WORKERS"] = str(len(args.direct_cpus))
    rows = []
    repeats = args.inner if mode in ("pub", "mix") else 1
    for part in range(repeats):
        output = Path(f"{prefix}-{part}.json")
        command = ["taskset", "-c", ",".join(map(str, args.direct_cpus)), str(args.direct), "-m", mode, "-w", str(len(args.direct_cpus)), "--json", str(output)]
        with open(f"{prefix}-{part}.log", "w") as log:
            subprocess.run(command, env=env, stdout=log, stderr=log, timeout=args.timeout, check=True)
        rows.extend(json.loads(output.read_text())["throughput"])
    return dict(transport="in-process", side="current", mode=mode, round=index, rows=grouped(rows))


def summarize(args, records, metadata):
    sections = {}
    for transport in ("tcp", "in-process"):
        section = {}
        selected = [r for r in records if r["transport"] == transport and r["round"] >= 0]
        for side in dict.fromkeys(r["side"] for r in selected):
            side_rows = {}
            for label in dict.fromkeys(label for r in selected if r["side"] == side for label in r["rows"]):
                values = [r["rows"][label]["ops_per_sec"] for r in selected if r["side"] == side and label in r["rows"]]
                stats = distribution(values)
                if label in README:
                    stats.update(readme_rate=README[label], versus_readme_pct=100 * (stats["median"] / README[label] - 1))
                side_rows[label] = stats
            section[side] = side_rows
        sections[transport] = section
    result = dict(metadata=metadata, results=sections, samples=records)
    (args.out / "summary.json").write_text(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", action="append", default=[], help="label=/absolute/path/to/server")
    parser.add_argument("--tcp-bench", type=Path)
    parser.add_argument("--direct", type=Path)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--inner", type=int, default=3)
    parser.add_argument("--modes", default="key,mix,pub")
    parser.add_argument("--workers", type=int)
    parser.add_argument("--server-cpus", type=cpus)
    parser.add_argument("--client-cpus", type=cpus)
    parser.add_argument("--direct-cpus", type=cpus, default=sorted(os.sched_getaffinity(0)))
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--hotkey-seconds", type=float, default=5)
    parser.add_argument("--wait-idle", type=float, default=0, help="wait this many seconds for CPU/memory pressure to settle, or abort")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out = args.out.resolve()
    if (args.out / "metadata.json").exists():
        parser.error("output already contains a run; choose a fresh --out directory")
    args.out.mkdir(parents=True, exist_ok=True)
    args.server_cpus = args.server_cpus or default_cpus()[0]
    args.client_cpus = args.client_cpus or default_cpus()[1]
    args.workers = args.workers or len(args.server_cpus)
    if args.runs < 3 or args.inner < 1 or args.warmups < 0 or args.hotkey_seconds <= 0 or args.wait_idle < 0:
        parser.error("use at least three runs, positive inner count, and nonnegative warmups")
    servers = dict(item.split("=", 1) for item in args.server)
    servers = {label: Path(path).resolve() for label, path in servers.items()}
    if not servers and not args.direct:
        parser.error("provide --server or --direct")
    if servers and not args.tcp_bench:
        parser.error("--server requires --tcp-bench")
    if args.tcp_bench:
        args.tcp_bench = args.tcp_bench.resolve()
    if args.direct:
        args.direct = args.direct.resolve()
    binaries = dict(servers)
    if args.direct:
        binaries["direct-bench"] = args.direct
    if args.tcp_bench:
        binaries["tcp-bench"] = args.tcp_bench
    cpu_model = next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name"))
    metadata = dict(status="running", cpu=cpu_model, initial_pressure=pressure(), governor=Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read_text().strip(),
                    server_cpus=args.server_cpus, client_cpus=args.client_cpus, direct_cpus=args.direct_cpus, workers=args.workers, runs=args.runs, warmups=args.warmups, inner=args.inner,
                    hotkey_duration_seconds=args.hotkey_seconds if "hotkey" in args.modes.split(",") else None,
                    counts=dict(clients=100, kv_per_client=100000, mixed_per_client=20000, hotkey_per_client=100000, subscribers=50, publishers=10, messages_per_publisher=20000),
                    binary_sha256={label: hashlib.sha256(path.read_bytes()).hexdigest() for label, path in binaries.items()},
                    readme_basis="Historical best-of-three claims with conflicting TCP/in-process labels; rates are references, not matched measurements.")
    (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2))
    root = Path(__file__).resolve().parents[1]
    records = []
    try:
        for mode in args.modes.split(","):
            if args.direct and mode != "hotkey":
                for index in range(-args.warmups, args.runs):
                    row = run_direct(args, mode, index)
                    records.append(row)
                    summarize(args, records, metadata)
                    print(f"direct {mode} round={index} complete", flush=True)
            for index in range(-args.warmups, args.runs):
                order = list(servers)
                if index % 2:
                    order.reverse()
                for side in order:
                    row = run_tcp(args, side, servers[side], mode, index, root)
                    records.append(row)
                    summarize(args, records, metadata)
                    print(f"tcp {side} {mode} round={index} complete", flush=True)
                    time.sleep(.2)
        metadata["status"] = "complete"
    except BaseException as error:
        metadata["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        metadata["error"] = type(error).__name__
        raise
    finally:
        summarize(args, records, metadata)
        (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2))
    print(f"Results: {args.out / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
