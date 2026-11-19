#!/usr/bin/env python3
"""Run the normal Go suite with equal-priority Docker server/client processes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import statistics
import subprocess
import threading
import time
import uuid

from bench_stable import cpus, default_cpus, pressure, rss


def validate_plan(server_cpus, client_cpus, workers, allow_shared=False):
    if not server_cpus or not client_cpus or not workers:
        raise ValueError('server CPUs, client CPUs and worker counts must be nonempty')
    if any(n < 1 or n > len(server_cpus) for n in workers):
        raise ValueError('worker counts must fit the server CPU allocation')
    shared = bool(set(server_cpus) & set(client_cpus))
    if shared and not allow_shared:
        raise ValueError('client/server CPUs overlap; use --allow-shared-cpus to label that separate workload')
    return shared


def verify_priorities(server_pid, client_pid):
    values = {name: os.getpriority(os.PRIO_PROCESS, pid)
              for name, pid in [('server', server_pid), ('client', client_pid)]}
    if values['server'] != values['client']:
        raise RuntimeError(f'benchmark priorities differ: {values}')
    return values


def docker(*args, check=True, timeout=None):
    return subprocess.run(['docker', *map(str, args)], capture_output=True, text=True,
                          check=check, timeout=timeout)


def inspect(name):
    return json.loads(docker('inspect', name).stdout)[0]


def group(name):
    pid = inspect(name)['State']['Pid']
    relative = Path(f'/proc/{pid}/cgroup').read_text().strip().split('::')[-1]
    return Path('/sys/fs/cgroup') / relative.lstrip('/')


def memory(path, filename='memory.current'):
    try:
        return int((path / filename).read_text())
    except OSError:
        return 0


def high_water(pid):
    try:
        return int(next(line.split()[1] for line in Path(f'/proc/{pid}/status').read_text().splitlines()
                        if line.startswith('VmHWM:')))
    except (OSError, StopIteration):
        return 0


def command(port, wire):
    with socket.create_connection(('127.0.0.1', port), timeout=1) as stream:
        stream.sendall(wire)
        return stream.makefile('rb').readline()


def summarize(records, args):
    result = {}
    for label in dict.fromkeys(r['server'] for r in records):
        result[label] = {}
        for workers in args.workers:
            rows = [r for r in records if r['server'] == label and r['workers'] == workers
                    and not r['warmup'] and r['status'] == 'passed']
            if not rows:
                continue
            def distribution(values):
                return dict(median=statistics.median(values), low=min(values), high=max(values))
            metrics = {t['label']: distribution([next(v['ops_per_sec'] for v in r['throughput']
                                                      if v['label'] == t['label']) for r in rows])
                       for t in rows[0]['throughput']}
            result[label][str(workers)] = dict(samples=len(rows), throughput=metrics,
                peak_RSS_MiB=distribution([r['peak_rss_kib']/1024 for r in rows]),
                peak_container_MiB=distribution([r['peak_container_bytes']/1048576 for r in rows]))
    (args.out / 'summary.json').write_text(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', action='append', required=True, help='label=Docker-image')
    parser.add_argument('--client', type=Path, help='static Go client; otherwise build bench/ with CGO_ENABLED=0')
    parser.add_argument('--workers', type=cpus, default=[1, 2, 3, 6])
    parser.add_argument('--server-cpus', type=cpus)
    parser.add_argument('--client-cpus', type=cpus)
    parser.add_argument('--allow-shared-cpus', action='store_true')
    parser.add_argument('--memory', default='4g')
    parser.add_argument('--client-memory', default='1g')
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--mode', choices=['key', 'bench'], default='bench')
    parser.add_argument('--timeout', type=float, default=600)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.server_cpus = args.server_cpus or default_cpus()[0]
    args.client_cpus = args.client_cpus or default_cpus()[1]
    try:
        shared = validate_plan(args.server_cpus, args.client_cpus, args.workers, args.allow_shared_cpus)
    except ValueError as error:
        parser.error(str(error))
    if args.runs < 3 or args.warmups < 0:
        parser.error('use at least three measured runs and nonnegative warmups')
    if not (set(args.server_cpus) | set(args.client_cpus)) <= os.sched_getaffinity(0):
        parser.error('requested CPUs are outside the allowed CPU set')
    if any(not re.fullmatch(r'[1-9][0-9]*[kmgtKMGT]?', value)
           for value in [args.memory, args.client_memory]):
        parser.error('memory limits must be positive Docker memory sizes')
    servers = dict(item.split('=', 1) for item in args.server)
    if not servers or any(not re.fullmatch(r'[A-Za-z0-9_-]+', label) for label in servers):
        parser.error('server labels must contain only letters, digits, underscore or hyphen')
    args.out = args.out.resolve()
    if args.out.exists() and any(args.out.iterdir()):
        parser.error('choose a fresh output directory; previous results are never overwritten')
    args.out.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    if args.client:
        client = args.client.resolve()
    else:
        client = args.out / 'fyrobench'
        subprocess.run(['go', 'build', '-trimpath', '-o', str(client), '.'], cwd=repo/'bench',
                       env={**os.environ, 'CGO_ENABLED': '0'}, check=True)
    images = {label: json.loads(docker('image', 'inspect', tag).stdout)[0]['Id']
              for label, tag in servers.items()}
    client_image = next(iter(images.values()))
    metadata = dict(status='running', images=images, client_image=client_image,
        client_sha256=hashlib.sha256(client.read_bytes()).hexdigest(),
        go_source_sha256={str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (repo/'bench').glob('*.go')},
        server_cpus=args.server_cpus, client_cpus=args.client_cpus, workers=args.workers,
        shared_cpus=shared, runs=args.runs, warmups=args.warmups, mode=args.mode,
        server_memory_limit=args.memory, client_memory_limit=args.client_memory,
        harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        cpu_topology={str(cpu): {
            name: (Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')/name).read_text().strip()
            for name in ['physical_package_id', 'core_id', 'thread_siblings_list']}
            for cpu in sorted(set(args.server_cpus) | set(args.client_cpus))},
        notes=['Unmodified Go client and default operation counts.',
               'Both processes run in Docker; equal niceness is checked before releasing the client.',
               'Shared-CPU runs require an explicit flag and are separate from dedicated-client scaling.',
               'Only the two owned benchmark containers share a PID namespace; host PID namespace is not shared.',
               'Server RSS uses kernel VmHWM; container memory uses memory.peak; client memory is sampled before cgroup exit.',
               'One complete warmup is retained but excluded; every measured attempt is retained.'])
    records = []
    try:
        for repeat in range(-args.warmups, args.runs):
            configurations = args.workers if repeat % 2 == 0 else list(reversed(args.workers))
            for workers in configurations:
                order = list(images) if repeat % 2 == 0 else list(reversed(images))
                for label in order:
                    prefix = f'{label}-w{workers}-r{repeat}'
                    case = args.out / prefix
                    case.mkdir()
                    name = 'fyro-normal-' + uuid.uuid4().hex[:12]
                    client_name = 'fyro-load-' + uuid.uuid4().hex[:12]
                    row = dict(server=label, workers=workers, repeat=repeat, warmup=repeat < 0,
                               image=images[label], status='failed', host_pressure_before=pressure())
                    done = threading.Event()
                    sampler = None
                    samples = []
                    print(time.strftime('%H:%M:%S'), prefix, 'start', flush=True)
                    try:
                        with socket.socket() as reservation:
                            reservation.bind(('127.0.0.1', 0))
                            port = reservation.getsockname()[1]
                        server_cpus = args.server_cpus[:workers]
                        launch = ['run', '-d', '--name', name, '--network', 'host', '--cpuset-cpus',
                                  ','.join(map(str, server_cpus)), '--memory', args.memory,
                                  '--memory-swap', args.memory, '--ulimit', 'nofile=65536:65536']
                        env = dict(FYRODB_BIND='127.0.0.1', FYRODB_PORT=port, FYRODB_WORKERS=workers,
                                   FYRODB_WORKER_CORES=','.join(map(str, server_cpus)), FYRODB_MAX_KEYS=0,
                                   FYRODB_RDB_INTERVAL=0, FYRODB_RDB_PATH='/tmp/benchmark.rdb')
                        for key, value in env.items():
                            launch += ['-e', f'{key}={value}']
                        launch += [images[label]]
                        docker(*launch)
                        row['server_command'] = ['docker', *launch]
                        deadline = time.monotonic() + 10
                        while True:
                            try:
                                if command(port, b'*1\r\n$4\r\nPING\r\n') == b'+PONG\r\n':
                                    break
                            except OSError:
                                pass
                            if time.monotonic() >= deadline or not inspect(name)['State']['Running']:
                                raise RuntimeError('server startup failed')
                            time.sleep(.05)
                        pid = inspect(name)['State']['Pid']
                        server_group = group(name)
                        launch = ['run', '-d', '--name', client_name, '--network', 'host', '--pid',
                                  'container:'+name, '--cpuset-cpus', ','.join(map(str, args.client_cpus)),
                                  '--memory', args.client_memory, '--memory-swap', args.client_memory,
                                  '--user', f'{os.getuid()}:{os.getgid()}', '-e',
                                  f'GOMAXPROCS={len(args.client_cpus)}', '-v', f'{client}:/benchmark:ro',
                                  '-v', f'{case}:/results', '--entrypoint', 'sh', client_image, '-c',
                                  'while [ ! -e /results/go ]; do sleep 0.01; done; exec /benchmark "$@"',
                                  'fyrobench', '-p', str(port), '-pid', '1', '-m', args.mode,
                                  '-json', '/results/result.json']
                        docker(*launch)
                        row['client_command'] = ['docker', *launch]
                        client_pid = inspect(client_name)['State']['Pid']
                        row['niceness'] = verify_priorities(pid, client_pid)
                        client_group = group(client_name)
                        def sample():
                            while not done.wait(.02):
                                samples.append((rss(pid), memory(server_group), memory(client_group)))
                        sampler = threading.Thread(target=sample)
                        sampler.start()
                        (case/'go').touch()
                        result = docker('wait', client_name, timeout=args.timeout)
                        if result.stdout.strip() != '0':
                            raise RuntimeError('client exit: '+result.stdout.strip())
                        rows = json.loads((case/'result.json').read_text())['throughput']
                        if len(rows) != (3 if args.mode == 'key' else 20):
                            raise RuntimeError('incomplete benchmark phase list')
                        for item in rows:
                            expected = (10_000_000 if item['label'] in ['Pipeline-64 SET', 'Pipelined SET',
                                'Pipelined GET', 'Hot Key (1 key, contention)', 'Pub/Sub delivery']
                                else 200_000 if item['label'] == 'Pub/Sub publish' else 2_000_000)
                            if item['ops'] != expected or item['ops_per_sec'] <= 0:
                                raise RuntimeError('invalid completed operation count: '+item['label'])
                        expected_keys = 10_000_000 if args.mode == 'key' else 0
                        if command(port, b'*1\r\n$6\r\nDBSIZE\r\n') != f':{expected_keys}\r\n'.encode():
                            raise RuntimeError('final key count mismatch')
                        row.update(status='passed', throughput=rows,
                            peak_rss_kib=max([v[0] for v in samples]+[high_water(pid)]),
                            peak_container_bytes=max([v[1] for v in samples]+[memory(server_group, 'memory.peak')]))
                        print(time.strftime('%H:%M:%S'), prefix, 'passed',
                              [(v['label'], round(v['ops_per_sec']/1e6, 2)) for v in rows[:3]], flush=True)
                    except BaseException as error:
                        row['error'] = repr(error)
                        raise
                    finally:
                        done.set()
                        if sampler:
                            sampler.join()
                        if samples:
                            row.setdefault('peak_rss_kib', max(v[0] for v in samples))
                            row.setdefault('peak_container_bytes', max(v[1] for v in samples))
                            (case/'memory-samples.json').write_text(json.dumps(samples))
                        row['host_pressure_after'] = pressure()
                        for container in [client_name, name]:
                            info = docker('inspect', container, check=False)
                            if info.returncode == 0:
                                row['client_state' if container == client_name else 'state'] = json.loads(info.stdout)[0]['State']
                            log = docker('logs', container, check=False)
                            (case/('client.log' if container == client_name else 'server.log')).write_text(log.stdout+log.stderr)
                            docker('rm', '-f', '-v', container, check=False)
                        records.append(row)
                        (args.out/'records.json').write_text(json.dumps(records, indent=2))
        metadata['status'] = 'complete'
    except BaseException as error:
        metadata.update(status='failed', error=repr(error))
        raise
    finally:
        summarize(records, args)
        (args.out/'metadata.json').write_text(json.dumps(metadata, indent=2))
    print('Results:', args.out/'summary.json', flush=True)


if __name__ == '__main__':
    main()
