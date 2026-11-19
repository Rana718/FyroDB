#!/usr/bin/env python3
"""Run isolated Docker tuning and single/multicore regression comparisons."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import threading
import time
import uuid
from types import SimpleNamespace

from bench_stable import default_cpus, distribution, environment, pressure, rss, cpu_seconds, wait_idle

ROOT = Path(__file__).resolve().parents[1]
LABEL = 'fyrodb-benchmark=next-optimization'


def docker(*args, check=True):
    return subprocess.run(['docker', *map(str, args)], capture_output=True, text=True, check=check)


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def command(port, *parts):
    parts = [p if isinstance(p, bytes) else str(p).encode() for p in parts]
    wire = b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n' % len(p) + p + b'\r\n' for p in parts)
    with socket.create_connection(('127.0.0.1', port), timeout=30) as stream:
        stream.sendall(wire)
        with stream.makefile('rb') as reader:
            def read():
                line = reader.readline()
                if not line:
                    raise RuntimeError('connection closed')
                if not line.endswith(b'\r\n'):
                    raise RuntimeError('truncated RESP header')
                kind, value = line[:1], line[1:-2]
                if kind == b'$':
                    n = int(value)
                    if n < 0:
                        return None
                    payload = reader.read(n + 2)
                    if len(payload) != n + 2 or not payload.endswith(b'\r\n'):
                        raise RuntimeError('truncated RESP payload')
                    return payload[:-2]
                if kind == b'*':
                    n = int(value)
                    return None if n < 0 else [read() for _ in range(n)]
                if kind == b':':
                    return int(value)
                if kind == b'-':
                    raise RuntimeError(value.decode(errors='replace'))
                return value
            return read()


def core_sets():
    server, client = default_cpus()
    return {'single': [server[0]], 'multi': server, 'maximum': sorted(os.sched_getaffinity(0))}, client


def start(image, side, cores, options, name, mount=None):
    port = free_port()
    argv = ['run', '-d', '--name', name, '--label', LABEL, '--network', 'host', '--cpuset-cpus', ','.join(map(str, cores)), '--memory', '3g', '--memory-swap', '3g', '--ulimit', 'nofile=65536:65536', '--ulimit', 'memlock=-1', '--ulimit', 'core=0']
    if mount:
        argv += ['--mount', f'type=bind,source={mount},target=/data', '--user', f'{os.getuid()}:{os.getgid()}']
    if side == 'redis':
        argv += [image, 'redis-server', '--bind', '127.0.0.1', '--port', str(port), '--protected-mode', 'no', '--save', '', '--appendonly', 'no', '--io-threads', '1', '--maxmemory', '0', '--maxmemory-policy', 'noeviction']
    elif side != 'dragonfly':
        settings = dict(FYRODB_PORT=port, FYRODB_BIND='127.0.0.1', FYRODB_WORKERS=len(cores), FYRODB_WORKER_CORES=','.join(map(str, cores)), FYRODB_RDB_INTERVAL=0, FYRODB_RDB_PATH='/tmp/benchmark.rdb')
        settings.update(options)
        for k, v in settings.items():
            argv += ['-e', f'{k}={v}']
        argv += [image]
    else:
        argv += [image, 'dragonfly', '--bind=127.0.0.1', f'--port={port}', f'--proactor_threads={len(cores)}', '--force_epoll=true', '--maxmemory=0', '--cache_mode=false', '--dbfilename=', '--dbnum=1', '--tcp_nodelay=true', '--multi_exec_squash=true', '--primary_port_http_enabled=false', '--admin_nopass=true', '--logtostderr', *options]
    docker(*argv)
    for _ in range(300):
        try:
            if command(port, 'PING') == b'PONG':
                break
        except (OSError, RuntimeError):
            pass
        state = json.loads(docker('inspect', name).stdout)[0]['State']
        if not state['Running']:
            raise RuntimeError(docker('logs', name, check=False).stderr)
        time.sleep(.02)
    else:
        raise RuntimeError('startup deadline exceeded')
    comm = {'dragonfly': 'dragonfly', 'redis': 'redis-server'}.get(side, 'fyro_db')
    pid = next(int(s.split()[0]) for s in docker('top', name, '-eo', 'pid,comm').stdout.splitlines()[1:] if s.split()[-1] == comm)
    return port, pid, argv


def run_case(out, records, images, side, core, cores, client, config, options, profile, index, bench):
    key = (side, core, config, profile['name'], index)
    if any(tuple(r[k] for k in ['side', 'core', 'config', 'profile', 'round']) == key for r in records):
        return
    name = 'fyro-next-' + uuid.uuid4().hex[:12]
    stem = f'{side}-{core}-{config}-{profile["name"]}-{index}'
    row = dict(side=side, core=core, config=config, profile=profile['name'], round=index, status='failed', image=images[side], pressure_before=pressure())
    stop = threading.Event()
    samples = []
    monitor = None
    began = time.monotonic()
    try:
        row['host_before'] = wait_idle(SimpleNamespace(wait_idle=60))
        port, pid, launch = start(images[side], side, cores, options, name)
        row.update(launch=launch, port=port, pid=pid)
        def sample():
            while not stop.wait(.025):
                samples.append(rss(pid))
        monitor = threading.Thread(target=sample)
        monitor.start()
        cpu_start = cpu_seconds(pid)
        row['idle_rss_kib'] = rss(pid)
        if profile['name'] == 'connections':
            streams = [socket.create_connection(('127.0.0.1', port), timeout=10) for _ in range(2000)]
            time.sleep(.2)
            row['connected_rss_kib'] = rss(pid)
            for stream in streams:
                stream.sendall(b'*1\r\n$4\r\nPING\r\n')
                with stream.makefile('rb') as reader:
                    assert reader.read(7) == b'+PONG\r\n'
            row['pinged_rss_kib'] = rss(pid)
            for stream in streams:
                stream.close()
        elif profile['name'] == 'isolation':
            code = "import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);import client;client.PORT=int(sys.argv[2]);from isolation_e2e import stress;r=[stress(m,15) for m in ['lua','multi','watch']];Path(sys.argv[3]).write_text(json.dumps(r));sys.exit(not all(x['passed'] for x in r))"
            output = out / (stem + '.json')
            argv = ['taskset', '-c', ','.join(map(str, client)), 'python3', '-c', code, str(ROOT/'tests/e2e/python'), str(port), str(output)]
            row['command'] = argv
            with (out/(stem+'.log')).open('w') as log:
                result = subprocess.run(argv, stdout=log, stderr=log, timeout=180)
            row['isolation'] = json.loads(output.read_text())
            row['exit_code'] = result.returncode
            assert result.returncode == 0, 'isolation invariant failed'
        else:
            env = environment()
            env['GOMAXPROCS'] = str(len(client))
            env['PYTHONDONTWRITEBYTECODE'] = '1'
            for k in ['MIX_ONLY', 'MIX_REPEAT', 'ANN_READY_RECALL']:
                env.pop(k, None)
            env.update(profile.get('env', {}))
            row['data'] = []
            row['commands'] = []
            for inner in range(profile.get('inner', 1)):
                output = out/(stem+f'-{inner}.json')
                argv = ['taskset', '-c', ','.join(map(str, client)), str(bench), '-p', str(port), '-pid', str(pid), '-json', str(output), *profile['args']]
                row['commands'].append(argv)
                with (out/(stem+f'-{inner}.log')).open('w') as log:
                    result = subprocess.run(argv, env=env, stdout=log, stderr=log, timeout=profile.get('timeout', 300))
                row['exit_code'] = result.returncode
                if output.exists():
                    row['data'].append(json.loads(output.read_text()))
                assert result.returncode == 0, 'benchmark client failed'
            if profile['name'] == 'key':
                count = 10_000_000
                assert command(port, 'DBSIZE') == count, 'KV key count changed'
                rng = random.Random(17)
                ids = [0, count-1, *[rng.randrange(count) for _ in range(1000)]]
                assert command(port, 'MGET', *map(str, ids)) == [b'value']*len(ids), 'KV value verification failed'
                row['validated_keys'] = len(ids)
        row['cpu_seconds'] = cpu_seconds(pid)-cpu_start
        row['ping_after'] = command(port, 'PING').decode()
        row['state'] = json.loads(docker('inspect', name).stdout)[0]['State']
        assert row['state']['Running'] and not row['state']['OOMKilled'] and row['ping_after'] == 'PONG'
        row['status'] = 'passed'
    except Exception as exc:
        row['error'] = repr(exc)
    finally:
        row['wall_seconds'] = time.monotonic()-began
        stop.set()
        if monitor:
            monitor.join()
        row['peak_rss_kib'] = max([*samples, *[row.get(k, 0) for k in ['idle_rss_kib', 'connected_rss_kib', 'pinged_rss_kib']]])
        state = docker('inspect', name, check=False)
        if state.returncode == 0:
            row['state'] = json.loads(state.stdout)[0]['State']
        log = docker('logs', name, check=False)
        (out/(stem+'.server.log')).write_text(log.stdout+log.stderr)
        docker('rm', '-f', '-v', name, check=False)
        row['pressure_after'] = pressure()
        records.append(row)
        (out/'records.json').write_text(json.dumps(records, indent=2))
    print(side, core, config, profile['name'], index, row['status'], round(row['wall_seconds'], 2), flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--stage', choices=['tune', 'compare'], required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--bench', type=Path, default=ROOT/'temp/hot-path-review/bench-final')
    p.add_argument('--current', default='fyrodb:next-candidate')
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--cores', default='single,multi')
    p.add_argument('--only', default='')
    args = p.parse_args()
    args.bench = args.bench.resolve()
    if not args.bench.is_file():
        args.bench.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(['go', 'build', '-o', str(args.bench), '.'], cwd=ROOT/'bench', check=True)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    images = {side:json.loads(docker('image','inspect',tag).stdout)[0]['Id'] for side,tag in [('baseline','fyrodb:hot-path-final'),('current',args.current),('dragonfly','docker.dragonflydb.io/dragonflydb/dragonfly:latest')]}
    cores, client = core_sets()
    metadata = dict(images=images, cores=cores, client_cpus=client, memory_limit=3*1024**3, swap=0, persistence=False, bench_sha256=hashlib.sha256(args.bench.read_bytes()).hexdigest(), targets={'Pipelined GET':60e6,'Pipelined SET':50e6,'Pipeline-64 SET':30e6}, notes=['Maximum uses all allowed server CPUs and shares two physical cores with the load generator.','Single and multi use separate client physical cores.','All performance containers have fresh empty data and no eviction.','The KV phase names distinguish cold pipeline-64 insertion from warm deep-pipeline overwrites and reads.'])
    if (out/'metadata.json').exists():
        assert json.loads((out/'metadata.json').read_text()) == metadata, 'configuration changed; use a new output directory'
    else:
        (out/'metadata.json').write_text(json.dumps(metadata,indent=2))
    records = json.loads((out/'records.json').read_text()) if (out/'records.json').exists() else []
    key = dict(name='key',args=['-m','key'])
    tuning = {'baseline': [('reference',{})], 'current':[('default',{}),('shards8',{'FYRODB_SHARDS':8}),('shards64',{'FYRODB_SHARDS':64}),('shards256',{'FYRODB_SHARDS':256})], 'dragonfly':[('reference',['--pipeline_squash=10']),('default',['--pipeline_squash=1']),('shared',['--pipeline_squash=1','--enable_shared_read_buffer=true','--request_cache_limit=8388608']),('batch64',['--pipeline_squash=64','--max_client_iobuf_len=65536'])]}
    if args.stage == 'tune':
        for core in args.cores.split(','):
            for i in range(-1,args.rounds):
                cases=[(side,cfg,opts) for side,choices in tuning.items() for cfg,opts in choices if not (side=='current' and core=='multi' and cfg=='shards8')]
                if i%2: cases.reverse()
                for side,cfg,opts in cases:
                    run_case(out,records,images,side,core,cores[core],client,cfg,opts,key,i,args.bench)
    else:
        selection=json.loads((ROOT/'temp/next-optimization/selection.json').read_text())
        profiles=[dict(name='key',args=['-m','key'],inner=3),dict(name='mix',args=['-m','mix'],env={'MIX_REPEAT':'1'},timeout=900),dict(name='pub',args=['-m','pub']),dict(name='latency-p1',args=['-m','lat','-dur','10s','-pipe','1']),dict(name='latency-p64',args=['-m','lat','-dur','10s','-pipe','64']),dict(name='connections')]
        for core in args.cores.split(','):
            for profile in profiles:
                if args.only and profile['name'] not in args.only.split(','):
                    continue
                for i in range(-1 if profile['name'] in ['key','mix','pub'] else 0,args.rounds):
                    sides=['baseline','current','dragonfly']
                    if i%2:sides.reverse()
                    for side in sides:
                        choice=selection.get(core,selection['multi']).get(side,{'name':'reference','options':{}})
                        run_case(out,records,images,side,core,cores[core],client,choice['name'],choice['options'],profile,i,args.bench)
            more=[dict(name='isolation'),dict(name='memory',args=['-m','mem']),dict(name='vector',args=['-m','vec'],timeout=600),dict(name='ann-hnsw',args=['-m','ann','-ann-synth-n','100000','-ann-synth-dim','128','-ann-q','1000'],timeout=600),dict(name='ann-flat',args=['-m','ann','-ann-algo','FLAT','-ann-synth-n','100000','-ann-synth-dim','128','-ann-q','1000'],timeout=600)]
            for profile in more:
                if args.only and profile['name'] not in args.only.split(','):
                    continue
                for i in range(args.rounds if profile['name'] in ['ann-hnsw','ann-flat','memory'] else 1):
                    sides=['baseline','current','dragonfly']
                    if i%2:sides.reverse()
                    for side in sides:
                        choice=selection.get(core,selection['multi']).get(side,{'name':'reference','options':{}})
                        run_case(out,records,images,side,core,cores[core],client,choice['name'],choice['options'],profile,i,args.bench)


if __name__ == '__main__':
    main()
