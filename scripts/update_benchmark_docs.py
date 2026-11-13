#!/usr/bin/env python3
"""Regenerate benchmark documentation and SVGs from the committed evidence."""
import argparse
import csv
import gzip
import hashlib
import html
import io
import json
from pathlib import Path
import re
import statistics
import textwrap
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/public/benchmarks/2026-11-13'
CHECK = argparse.ArgumentParser(description=__doc__)
CHECK.add_argument('--check', action='store_true')
ARGS = CHECK.parse_args()
GENERATED = {}


def read(path):
    data = (OUT / path).read_bytes()
    return json.loads(gzip.decompress(data) if path.endswith('.gz') else data)


def emit(path, content):
    if isinstance(content, str):
        content = content.encode()
    GENERATED[Path(path)] = content


def number(value, memory=False):
    return f'{value:.1f} MiB' if memory else f'{value / 1e6:.3f}M'


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(map(str, row)) + ' |' for row in rows])


def columns(filename, title, subtitle, names, entries, source):
    width = 1120
    gap = 22
    pad = 28
    column_width = (width - pad * 2 - gap * (len(names) - 1)) / len(names)
    row_height = 88
    height = 164 + len(entries) * row_height + 55
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
           f'<title id="title">{html.escape(title)}</title><desc id="desc">{html.escape(subtitle + ". " + source)}</desc>',
           '<style>text{font-family:Inter,system-ui,sans-serif;fill:#203943}.title{font-size:25px;font-weight:650}.sub{font-size:14px;fill:#55737c}.name{font-size:21px;font-weight:600}.label{font-size:14px;fill:#55737c}.value{font-size:24px;font-weight:600}.range{font-size:12px;fill:#657d84}</style>',
           f'<rect width="{width}" height="{height}" fill="#f5f8f8"/>',
           f'<text class="title" x="28" y="38">{html.escape(title)}</text>',
           f'<text class="sub" x="28" y="66">{html.escape(subtitle)}</text>']
    colors = ['#287c77', '#7a8996', '#796a95', '#7d714d', '#4b788d']
    for index, name in enumerate(names):
        x = pad + index * (column_width + gap)
        svg.append(f'<rect x="{x}" y="88" width="{column_width}" height="{height - 125}" rx="10" fill="white" stroke="#d4dfe1"/>')
        header_lines = textwrap.wrap(name, width=max(14, int((column_width - 36) / 11)))
        for line_index, line in enumerate(header_lines):
            svg.append(f'<text class="name" x="{x + 18}" y="{116 + line_index * 22}">{html.escape(line)}</text>')
        for row_index, (label, values, memory) in enumerate(entries):
            stat = values[index]
            assert all(stat[k] >= 0 for k in ['median', 'low', 'high'])
            assert stat['low'] <= stat['median'] <= stat['high']
            maximum = max(v['median'] for v in values) or 1
            y = 157 + row_index * row_height
            svg += [f'<text class="label" x="{x + 18}" y="{y}">{html.escape(label)}</text>',
                    f'<text class="value" x="{x + 18}" y="{y + 27}">{number(stat["median"], memory)}</text>',
                    f'<rect x="{x + 18}" y="{y + 37}" width="{column_width - 36}" height="6" rx="3" fill="#e9efef"/>',
                    f'<rect x="{x + 18}" y="{y + 37}" width="{(column_width - 36) * stat["median"] / maximum:.2f}" height="6" rx="3" fill="{colors[index]}"/>',
                    f'<text class="range" x="{x + 18}" y="{y + 61}">{html.escape("Range " + number(stat["low"], memory) + " – " + number(stat["high"], memory) if stat.get("range_available", True) else "Median only; raw attempts archived")}</text>']
    svg.append(f'<text class="range" x="28" y="{height - 14}">{html.escape(source)}</text></svg>')
    result = '\n'.join(svg) + '\n'
    ET.fromstring(result)
    emit(OUT / filename, result)


manifest = read('manifest.json')
for item in manifest['files']:
    data = (OUT / item['file']).read_bytes()
    assert hashlib.sha256(data).hexdigest() == item['stored_sha256'], item['file']
    raw = gzip.decompress(data) if item['file'].endswith('.gz') else data
    assert len(raw) == item['bytes'] and hashlib.sha256(raw).hexdigest() == item['sha256']

normal = read('evidence/scaling-verified-20261113/summary.json')
large = read('evidence/large-path-verified/summary.json')
experiment = read('evidence/set-admission-isolated/summary.json')
older_stock = read('evidence/standard-benchmark/summary.json')
validation = read('evidence/set-admission-isolated/retained-checks.json')
assert validation['workspace'] == dict(passed=789, failed=0, ignored=3)
image = manifest['runtime_image']
for folder, group in [('controlled', 'fixed_client'), ('shared', 'shared_client')]:
    rows = read(f'evidence/scaling-verified-20261113/{folder}/records.json.gz')
    for row in rows:
        assert row['image'] == image and row['status'] == 'passed'
        assert row['niceness'] == dict(server=0, client=0)
        assert not row['state']['OOMKilled'] and not row['client_state']['OOMKilled']
    for workers, summary in normal[group].items():
        timed = [r for r in rows if str(r['workers']) == workers and not r['warmup']]
        assert len(timed) == summary['samples'] == 3
        for key, stat in summary['throughput'].items():
            values = [next(v['ops_per_sec'] for v in r['throughput'] if v['label'] == key) for r in timed]
            assert stat == dict(median=statistics.median(values), low=min(values), high=max(values))

for folder, count in [('standard-final', 108), ('capacity-final', 36), ('hot-final', 24),
                      ('small-acceptance', 24), ('scale-final', 12), ('fresh-small', 12)]:
    rows = read(f'evidence/large-path-verified/{folder}/records.json.gz')
    assert len(rows) == count and all(r['status'] == 'passed' and not r['state']['OOMKilled'] for r in rows)
    assert all(r['image'] == image for r in rows if r['side'] == 'candidate')

configs = [normal['fixed_client'][v] for v in ['1', '2', '3', '6']] + [normal['shared_client']['12']]
names = ['1 worker', '2 workers', '3 workers', '6 workers / SMT', '12 / shared client']
short_keys = ['Pipelined GET', 'Pipelined SET']
entries = [(k, [c['throughput'][k] for c in configs], False) for k in short_keys]
entries.append(('Peak server RSS', [c['peak_RSS_MiB'] for c in configs], True))
columns('normal-scaling.svg', 'Current normal Go TCP suite · worker scaling',
        'Three-run medians; 12-worker clients share CPUs. Other clients use dedicated CPUs.', names, entries,
        'Source: scaling-verified-20261113. Throughput: M ops/s. Ranges: min–max. Same retained runtime.')
entries = [(k, [c['throughput'][k] for c in [configs[3], configs[4]]], False)
           for k in configs[0]['throughput']]
entries += [('Peak RSS', [c['peak_RSS_MiB'] for c in [configs[3], configs[4]]], True),
            ('Peak container memory', [c['peak_container_MiB'] for c in [configs[3], configs[4]]], True)]
columns('normal-all.svg', 'Every normal TCP workload · current runtime',
        'Six workers use 3 physical cores + SMT; 12 share all 6 physical cores with clients.',
        ['6 workers / dedicated clients', '12 workers / shared clients'], entries,
        'Source: scaling-verified-20261113. Three-run medians and ranges. Delivery uses M messages/s.')

for core in ['single', 'multi']:
    rows = sorted([r for r in large if r['mode'] == 'standard' and r['core'] == core], key=lambda r: r['size'])
    entries = []
    for row in rows:
        label = f'{row["size"]:,} B'
        for key in ['GET', 'SET', 'RAM']:
            entries.append((label + ' · ' + ('Peak RSS' if key == 'RAM' else key),
                            [row['versions'][s][key] for s in ['candidate', 'dragonfly']], key == 'RAM'))
    columns(f'stock-{core}.svg', 'Official redis-benchmark · ' + ('single worker' if core == 'single' else '6 workers'),
            'P256 · 64 connections · 4 client threads · 100,000 populated random keys · 3 rounds',
            ['FyroDB retained build', 'Dragonfly'], entries,
            'Source: large-path-verified / standard-final. M ops/s; RSS MiB. Stock-client rates can be client-limited.')

entries = []
for row in sorted([r for r in large if r['mode'] == 'standard' and r['core'] == 'single' and r['size'] >= 4096], key=lambda r:r['size']):
    for key in ['GET', 'SET', 'RAM']:
        entries.append((f'{row["size"]:,} B · ' + ('Peak RSS' if key == 'RAM' else key),
                        [row['versions'][s][key] for s in ['baseline', 'candidate']], key == 'RAM'))
columns('large-payload.svg', 'Retained large-payload optimization · single worker',
        'Matched before/after builds; all tested metrics shown, including unchanged or slower cases.',
        ['Before large-payload change', 'Retained FyroDB'], entries,
        'Source: large-path-verified / standard-final. Three-run medians and min–max ranges.')

entries = []
for row in sorted([r for r in large if r['mode'] == 'capacity'], key=lambda r:r['size']):
    for key in ['GET', 'SET', 'RAM']:
        entries.append((f'{row["size"]:,} B · ' + ('Peak RSS' if key == 'RAM' else key),
                        [row['versions'][s][key] for s in ['candidate', 'dragonfly']], key == 'RAM'))
columns('capacity.svg', 'Separate multi-process capacity diagnostic · 6 workers',
        'Six official clients; total completed requests / slowest reported client duration. Not the stock score.',
        ['FyroDB retained build', 'Dragonfly'], entries,
        'Source: large-path-verified / capacity-final. Same CPU budgets. Three-round medians/ranges; M ops/s and MiB.')

entries = []
for row in sorted([r for r in large if r['mode'].startswith('hot')], key=lambda r:(r['mode'],r['core'],r['size'])):
    for key in ['GET', 'SET']:
        label = f'{row["core"]} · P{1 if row["mode"] == "hot1" else 256} · {row["size"]:,} B · {key}'
        entries.append((label, [row['versions'][s][key] for s in ['baseline', 'candidate']], False))
columns('hot-key.svg', 'Large hot-key regression checks · one repeated key',
        'Single/six workers; scalar and deep pipelines. Repeated-key paths preserve guarded payload lifetime.',
        ['Before large-payload change', 'Retained FyroDB'], entries,
        'Source: large-path-verified / hot-final. Three-round medians and min–max; M ops/s.')

entries = []
for row in [r for r in experiment if r['kind'] in ['normal', 'shared']]:
    for key in ['Pipelined GET', 'Pipelined SET', 'RSS']:
        label = ('12 shared' if row['kind'] == 'shared' else row['core'] + ' workers') + ' · ' + ('Peak RSS' if key == 'RSS' else key.replace('Pipelined ', ''))
        entries.append((label, [row['versions'][s][key] for s in ['baseline', 'candidate']], key == 'RSS'))
columns('rejected-admission.svg', 'Rejected SET admission experiment · runtime reverted',
        'Correctness passed; throughput improvement was not established. Host noise affects both labels.',
        ['Retained checkpoint', 'Rejected experiment'], entries,
        'Source: set-admission-isolated. Same resources and niceness 0. Three-round medians and ranges.')

entries = []
for pipeline in [1, 16, 64, 256]:
    for command in ['GET', 'SET']:
        values = []
        for engine in ['fyrodb', 'dragonfly', 'redis']:
            row = next(r for r in older_stock['figures'] if r['core'] == 'single' and r['pipeline'] == pipeline and r['command'] == command and r['database'] == engine)
            values.append(dict(median=row['rps'], low=row['rps'], high=row['rps'], range_available=False))
        entries.append((f'P{pipeline} · {command}', values, False))
columns('redis-history.svg', 'Earlier standard-client comparison · single core',
        'Older FyroDB build, Dragonfly and standalone Redis. Median-only archive; range not encoded here.',
        ['Earlier FyroDB build', 'Dragonfly', 'Redis'], entries,
        'Source: standard-benchmark. Historical experiment, not the current d5a178b runtime. M ops/s.')

memtier = {}
for side in ['baseline','candidate']:
    rr = read(f'evidence/large-payload-optimization/memtier-probe/{side}/records.json.gz')
    assert len(rr) == 2 and all(r['status'] == 'passed' for r in rr)
    memtier[side] = {}
    for row in rr:
        totals = row['data']['ALL STATS']['Totals']
        assert totals['Connection Errors'] == 0
        value = totals['Ops/sec']
        memtier[side][row['command']] = dict(median=value, low=value, high=value, range_available=False)
columns('memtier-history.svg', 'Earlier independent memtier cross-check',
        '64 B · 4 threads × 16 clients · P64 · 5 seconds · one attempt per command/build',
        ['Earlier baseline build','Earlier candidate build'],
        [(key,[memtier[s][key] for s in ['baseline','candidate']],False) for key in ['GET','SET']],
        'Source: large-payload-optimization / memtier-probe. Historical smoke cross-check, not a current speedup.')

for row in experiment:
    if row['kind'] not in ['stock', 'scalar']:
        continue
    for version in row['versions'].values():
        assert all(version[k]['low'] <= version[k]['median'] <= version[k]['high'] for k in row['metrics'])

campaigns = []
for item in manifest['files']:
    if item['original'].endswith('/summary.json'):
        name = item['original'].split('/')[1]
        scope = ('current runtime reference' if name in ['scaling-verified-20261113','large-path-verified']
                 else 'rejected experiment' if name == 'set-admission-isolated'
                 else 'earlier measurement; not a current reference')
        campaigns.append(dict(name=name, scope=scope, summary=item['file']))

dataset = dict(archive_date=manifest['archive_date'], checkpoint=manifest['runtime_checkpoint'],
               runtime_image=image, build_labels=dict(large_payload='candidate is retained 3d5ef33d50f9; baseline is the earlier image', rejected_experiment='baseline is retained 3d5ef33d50f9; candidate was reverted'), normal=normal, large_payload=large, rejected_experiment=experiment,
               historical_standard=older_stock, historical_memtier=memtier, validation=validation, campaigns=campaigns)
emit(OUT/'results.json', json.dumps(dataset, indent=2)+'\n')
home_summary = dict(checkpoint='d5a178b', configurations=[
    dict(workers=n, get_mops=c['throughput']['Pipelined GET']['median']/1e6,
         set_mops=c['throughput']['Pipelined SET']['median']/1e6,
         rss_mib=c['peak_RSS_MiB']['median'],
         incr_mops=c['throughput']['INCR (atomic counters)']['median']/1e6,
         list_mops=c['throughput']['LPUSH/RPOP (queue)']['median']/1e6)
    for n,c in zip([1,2,3,6,12],configs)])
emit(ROOT/'docs/src/lib/benchmark-summary.ts',
     'export const normalBenchmark = '+json.dumps(home_summary,indent=2)+' as const;\n')

rows = []
for group, cs in normal.items():
    for workers, c in cs.items():
        for metric, stat in {**c['throughput'],'Peak RSS':c['peak_RSS_MiB'],'Container peak':c['peak_container_MiB']}.items():
            rows.append(['normal/'+group,workers,5,'retained',metric,*[stat[k] for k in ['median','low','high']],
                         'MiB' if metric in ['Peak RSS','Container peak'] else 'messages/s' if metric=='Pub/Sub delivery' else 'ops/s'])
for r in large:
    for side,v in r['versions'].items():
        for metric in ['GET','SET','RAM','container_RAM']:
            rows.append([r['mode'],r['core'],r['size'],side,metric,*[v[metric][k] for k in ['median','low','high']],
                         'MiB' if metric in ['RAM','container_RAM'] else 'ops/s'])
for r in experiment:
    for side,v in r['versions'].items():
        for metric in r['metrics']+['RSS','container']:
            rows.append(['rejected/'+r['kind'],r['core'],r['size'],side,metric,*[v[metric][k] for k in ['median','low','high']],
                         'MiB' if metric in ['RSS','container'] else 'messages/s' if metric=='Pub/Sub delivery' else 'ops/s'])
for r in older_stock['figures']:
    rows.append(['historical/standard/P'+str(r['pipeline']),r['core'],64,r['database'],r['command'],r['rps'],None,None,'ops/s'])
    rows.append(['historical/standard/P'+str(r['pipeline']),r['core'],64,r['database'],r['command']+' p99',r['p99'],None,None,'ms'])
for side,metrics in memtier.items():
    for command,v in metrics.items():
        rows.append(['historical/memtier','8 workers',64,side,command,v['median'],None,None,'ops/s'])
rows = [r+['range unavailable' if r[6] is None else 'sample min/max'] for r in rows]
buf=io.StringIO();w=csv.writer(buf, lineterminator='\n');w.writerow(['scope','workers_or_layout','value_bytes','build','metric','median','minimum','maximum','unit','range_source']);w.writerows(rows)
emit(OUT/'numbers.csv',buf.getvalue())

normal_rows = []
for metric in configs[0]['throughput']:
    normal_rows.append([metric]+[f'{c["throughput"][metric]["median"]/1e6:.3f}' for c in configs])
normal_rows += [[label]+[f'{c[key]["median"]:.1f}' for c in configs]
                for label,key in [('Peak RSS (MiB)','peak_RSS_MiB'),('Peak container (MiB)','peak_container_MiB')]]
normal_table = table(['Metric (M/s unless marked)','1 worker','2 workers','3 workers','6 / SMT','12 / shared'],normal_rows)
mdx_path = ROOT/'docs/src/content/benchmarks.mdx'
mdx=mdx_path.read_text()
pattern=r'(\{/\* GENERATED_NORMAL_START \*/\}\n).*?(\n\{/\* GENERATED_NORMAL_END \*/\})'
assert re.search(pattern,mdx,re.S), 'missing generated table anchors'
mdx=re.sub(pattern,lambda m:m[1]+normal_table+m[2],mdx,flags=re.S)
emit(mdx_path,mdx)
body=mdx.split('---',2)[2].lstrip()
body=re.sub(r'\{/\* (.*?) \*/\}',r'<!-- \1 -->',body)
body=body.replace('/benchmarks/2026-11-13/','docs/public/benchmarks/2026-11-13/')
body=body.replace('/docs/benchmark-history','docs/src/content/benchmark-history.mdx').replace('/docs/architecture','ARCHITECTURE.md')
emit(ROOT/'BENCHMARKS.md','# Benchmark results and methodology\n\n'+body)

figures=[('normal-scaling.svg','Current normal TCP worker sweep'),('normal-all.svg','All 20 normal TCP workloads'),
         ('stock-single.svg','Official client, single worker'),('stock-multi.svg','Official client, six workers'),
         ('large-payload.svg','Retained large-payload before/after'),('capacity.svg','Separate multi-process capacity'),
         ('hot-key.svg','Large hot-key guards'),('redis-history.svg','Earlier standalone Redis comparison'),
         ('memtier-history.svg','Earlier independent memtier probe'),
         ('rejected-admission.svg','Rejected admission experiment')]
links=''.join(f'<a href="#f{i}">{html.escape(title)}</a>' for i,(_,title) in enumerate(figures))
parts=['<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>FyroDB benchmark evidence</title>',
       '<style>body{margin:0;background:#f5f8f8;color:#203943;font:16px system-ui,sans-serif}main{max-width:1120px;margin:auto;padding:30px 18px}h1{font-size:32px;letter-spacing:-.03em}p{line-height:1.65;max-width:900px}a{color:#216c69}nav{display:flex;flex-wrap:wrap;gap:12px;margin:24px 0}figure{margin:40px 0}img{display:block;width:100%;height:auto;border:1px solid #d7e1e2;border-radius:10px}figcaption{padding:10px 0;font-size:14px}.notice{border-left:3px solid #7b999a;padding:12px 18px;background:#e8efef}.downloads{display:flex;flex-wrap:wrap;gap:20px}details{margin:30px 0}li{margin:10px 0}@media(max-width:650px){figure{overflow:auto}img{min-width:760px}h1{font-size:26px}}</style><main>',
       '<h1>FyroDB benchmark evidence</h1><p>Current retained runtime: checkpoint d5a178b. Normal Go TCP results appear first. Other clients, payloads and CPU budgets are separate experiments.</p>',
       '<p class="notice">The latest SET admission experiment was reverted: correctness passed but a reliable throughput gain was not established. Historical best-of-three rates remain historical. No measurements were rerun for this documentation update.</p>',
       '<div class="downloads"><a href="results.json">All selected results (JSON)</a><a href="numbers.csv">Complete numeric CSV</a><a href="manifest.json">Evidence hashes and source list</a></div><nav>'+links+'</nav>']
for i,(file,title) in enumerate(figures):
    parts.append(f'<figure id="f{i}"><h2>{html.escape(title)}</h2><img src="{file}" alt="{html.escape(title)}"><figcaption>SVG: <a href="{file}">{file}</a>. Full ranges and provenance are in the downloads.</figcaption></figure>')
parts.append('<details><summary>All archived measurement campaigns</summary><ul>')
for c in campaigns:
    parts.append(f'<li><a href="{c["summary"]}">{c["name"]}</a> — {c["scope"]}</li>')
parts.append('</ul></details><p>84 timed samples plus 8 full Go warmup suites in the latest rejected experiment; 789 retained-source tests passed, zero failures, three existing performance probes ignored. RSS and container memory are distinct metrics; no fixed RAM ceiling is claimed.</p></main></html>\n')
emit(OUT/'index.html','\n'.join(parts))

for path, data in GENERATED.items():
    if ARGS.check:
        assert path.is_file() and path.read_bytes()==data, f'stale generated file: {path.relative_to(ROOT)}'
    else:
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
print(f'{"Verified" if ARGS.check else "Generated"} {len(GENERATED)} outputs; {len(manifest["files"])} evidence hashes; {len(rows)} numeric rows; {len(figures)} SVGs.')
