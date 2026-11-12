#!/usr/bin/env python3
"""Render matched Docker samples, validation, and resource limits as offline HTML."""
import argparse
import csv
import html
import json
from pathlib import Path
import re
import statistics

from bench_stable import distribution, grouped, README


def table(headers, rows):
    return '<table><thead><tr>'+''.join('<th>'+html.escape(str(x))+'</th>' for x in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(x))+'</td>' for x in row)+'</tr>' for row in rows)+'</tbody></table>'


def rate(n):
    return f'{n/1e6:.2f}M' if n>=1e6 else f'{n/1e3:.2f}K' if n>=1e3 else f'{n:.2f}'


def aggregate(row):
    metrics=[]
    for data in row.get('data',[]):
        for key in ['throughput','latency']:
            for item in data.get(key,[]) or []:
                item=dict(item)
                item.setdefault('elapsed_ns',item['ops']/item['ops_per_sec']*1e9)
                metrics.append(item)
    return grouped(metrics)



def key_chart(core, stats):
    labels=[('Pipeline-64 SET','Cold SET, pipeline 64'),('Pipelined SET','Warm SET, deep pipeline'),('Pipelined GET','GET, deep pipeline')]
    colors={'baseline':'#9b663d','current':'#146b87','dragonfly':'#43834e'}
    maximum=max([60e6,*[v['median'] for (c,side,p,label),v in stats.items() if c==core and p=='key']])*1.12
    left,width=220,700
    svg='<svg viewBox="0 0 1040 390" role="img" aria-label="Median TCP throughput by workload and backend"><rect width="1040" height="390" fill="white"/>'
    for tick in range(5):
        x=left+width*tick/4
        svg+=f'<line x1="{x}" x2="{x}" y1="30" y2="330" stroke="#e0e6ec"/><text x="{x}" y="350" text-anchor="middle" font-size="12">{maximum*tick/4/1e6:.0f}</text>'
    for group,(metric,label) in enumerate(labels):
        y=45+group*95
        svg+=f'<text x="12" y="{y+25}" font-size="13">{label}</text>'
        for index,side in enumerate(colors):
            v=stats.get((core,side,'key',metric));yy=y+index*23
            if v:
                w=width*v['median']/maximum
                svg+=f'<rect x="{left}" y="{yy}" width="{w}" height="17" fill="{colors[side]}"/><text x="{left+w+5}" y="{yy+13}" font-size="12">{v["median"]/1e6:.2f}M</text>'
            else:
                svg+=f'<text x="{left+5}" y="{yy+13}" font-size="12">{side}: no validated rate</text>'
    for i,(side,color) in enumerate(colors.items()):
        x=280+i*180
        svg+=f'<rect x="{x}" y="10" width="14" height="14" fill="{color}"/><text x="{x+20}" y="22" font-size="12">{side}</text>'
    return svg+'<text x="570" y="380" text-anchor="middle" font-size="13">Million completed TCP requests per second</text></svg>'


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,required=True)
    args=p.parse_args();root=args.root.resolve()
    datasets=[]
    for directory in ['comparison','maximum','refinement-no-lazy']:
        path=root/directory
        if (path/'records.json').exists():
            datasets.append((directory,json.loads((path/'metadata.json').read_text()),json.loads((path/'records.json').read_text())))
    for core in ['single','multi']:
        for prefix in ['pgcache-','pgcache-final-']:
            path=root/(prefix+core)/'results.json'
            if path.exists():
                data=json.loads(path.read_text())
                records=[dict(r,core=core,config='selected') for r in data['records']]
                datasets.append((prefix+core,data['metadata'],records))
    if not datasets:
        raise SystemExit('No measured comparison dataset found')
    rows=[dict(r,dataset=name) for name,_,rs in datasets for r in rs]
    measured=[r for r in rows if r['round']>=0]
    preference={(r['core'],r['side'],r['profile']):r['dataset'] for r in measured if (r['dataset']=='refinement-no-lazy' or r['dataset'].startswith('pgcache-final-')) and r['side'] in ['baseline','current']}
    selected=[r for r in measured if preference.get((r['core'],r['side'],r['profile']),r['dataset'])==r['dataset']]
    summary={}
    sources={}
    metadata_by_dataset={name:meta for name,meta,_ in datasets}
    for row in selected:
        if row['status']!='passed':continue
        for label,value in aggregate(row).items():
            key=(row['core'],row['side'],row['profile'],label)
            summary.setdefault(key,[]).append(value['ops_per_sec'])
            sources[key]=metadata_by_dataset[row['dataset']]['images'].get(row['side'],'unknown')
    stats={key:distribution(vals) for key,vals in summary.items()}
    failures=[dict(side=r['side'],core=r['core'],profile=r['profile'],round=r['round'],status=r['status'],error=r.get('error',''),state=r.get('state',{})) for r in measured if r['status']!='passed']
    goals=[]
    targets={'Pipelined GET':60e6,'Pipelined SET':50e6,'Pipeline-64 SET':30e6}
    for (core,side,profile,label),s in stats.items():
        if side=='current' and core in ['multi','maximum'] and profile=='key' and label in targets:
            goals.append(dict(core=core,metric=label,target=targets[label],median=s['median'],met=s['median']>=targets[label],cv_pct=s['cv_pct']))
    payload=dict(datasets={name:meta for name,meta,_ in datasets},measured_runs=len(measured),failed_runs=failures,targets=goals,summary=[dict(core=c,side=s,profile=p,metric=m,**v) for (c,s,p,m),v in stats.items()])
    (root/'summary.json').write_text(json.dumps(payload,indent=2))
    with (root/'throughput.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['core','backend','profile','metric','median_ops_s','min_ops_s','max_ops_s','CV_pct','CI95_low','CI95_high'])
        for (c,s,p,m),v in stats.items():w.writerow([c,s,p,m,v['median'],v['minimum'],v['maximum'],v['cv_pct'],*v['median_ci95']])
    title='FyroDB and Dragonfly: protected operations, memory, and scaling'
    body='<h1>'+title+'</h1><p>Fresh, pinned Docker containers; no swap; identical per-case memory limits. Medians and variability are retained. Throughput targets apply to multicore and maximum settings; single-core runs provide the scaling reference. Targets are goals, not promised scores. Failed validation and unsupported features remain visible below.</p>'
    body+='<p>Revision provenance: the broad three-backend matrix used the first candidate. Final allocator and connection-slab refinements were then measured separately for FyroDB; where available, those samples replace earlier FyroDB samples in the tables. The full raw history remains below. Lazy prefetch storage was rejected after it increased connection-burst RSS. Native regression and data tests were rerun after the refinement.</p>'
    body+='<p><b>Outcome:</b> The throughput targets were not reached. Warm SET, GET, and stream rates improved in the tested multicore workloads, but the final cold pipeline-64 SET median was below its paired baseline. This is not a blanket no-regression result.</p>'
    body+='<h2>Implemented changes</h2><p>The baseline is the previous validated safe image, rather than the original crashing release. The current image adds smaller read-ahead chunks, direct stream-ID decimal encoding, and a FLAT arena without unused HNSW neighbor slots. Bulk vector encoding now holds its structural write lock and validates unique IDs before parallel writes. New regression tests cover ID widths, vector overwrite/scan atomicity, compact arena persistence/reordering, duplicate batch IDs, and connection/value memory budgets. Auxiliary vector-buffer accounting was corrected; compare resident memory alongside reported index bytes.</p>'
    body+='<h2>Targets</h2>'+table(['CPU setting','workload','target ops/s','current median ops/s','CV','met'],[[g['core'],g['metric'],rate(g['target']),rate(g['median']),f"{g['cv_pct']:.1f}%",g['met']] for g in goals])
    if (root/'worker-selection.json').exists():
        presets=json.loads((root/'worker-selection.json').read_text());values=[]
        for core,choices in presets.items():
            item=choices.get('current')
            if not item:continue
            for label,target in targets.items():
                value=item['key_metrics'][label]
                values.append([core,item['name'],label,rate(value),rate(target),value>=target])
        body+='<h2>Earlier-revision KV worker trials</h2><p>These worker trials used the first candidate image and are separate from the general all-case settings. They use the same CPU and memory limits, with worker-count and data-integrity results shown below. A KV winner is not a claim that the setting wins every workload.</p>'+table(['CPU','preset','metric','median ops/s','target','met'],values)
    body+='<p>Pipeline-64 SET inserts a new keyspace; deep-pipeline SET overwrites it and GET reads it. These are different costs. Each key phase uses 100 clients × 100,000 operations. A measured key row aggregates three complete client runs, and three independent rows are compared. Worker-preset and safety follow-up records show their own run configuration. Pipeline-1 latency is reported separately.</p>'
    body+='<label>CPU setting <select id="core"><option value="all">All</option>'+''.join('<option>'+html.escape(c)+'</option>' for c in dict.fromkeys(r['core'] for r in rows))+'</select></label>'
    for core in dict.fromkeys(r['core'] for r in rows):
        body+='<section data-core="'+core+'"><h2>'+core+' CPU results</h2>'+key_chart(core,stats)
        if core in ['multi','maximum']:body+='<p><b>Dragonfly correctness note:</b> The fast multi-shard preset failed the MULTI snapshot check. The single-shard preset passed repeated isolation checks at lower throughput; see the safety comparison below. The 12-thread maximum preset was rejected at startup under the 3 GiB budget.</p>'
        values=[]
        for (c,s,p,m),v in stats.items():
            if c==core:values.append([s,p,m,sources[c,s,p,m][:19],len(v['samples']),rate(v['median']),rate(v['minimum']),rate(v['maximum']),f"{v['cv_pct']:.1f}%",f"{rate(v['median_ci95'][0])} – {rate(v['median_ci95'][1])}"])
        body+=table(['backend','profile','metric','image revision','runs','median ops/s','min','max','CV','bootstrap median CI95'],values)
        lat=[]
        for side in ['baseline','current','dragonfly']:
            for profile in ['latency-p1','latency-p64']:
                vals=[d['latency'][0] for r in selected if r['core']==core and r['side']==side and r['profile']==profile and r['status']=='passed' for d in r.get('data',[]) if d.get('latency')]
                if vals:lat.append([side,profile,rate(statistics.median(v['ops_per_sec'] for v in vals)),round(statistics.median(v['p99_ns'] for v in vals)/1e6,3),round(statistics.median(v['p999_ns'] for v in vals)/1e6,3)])
        if lat:body+='<h3>Latency: median of run percentiles</h3>'+table(['backend','scenario','ops/s','p99 ms','p99.9 ms'],lat)
        resources=[]
        for side in ['baseline','current','dragonfly']:
            idle=[r for r in selected if r['core']==core and r['side']==side and r['profile']=='connections' and r['status']=='passed']
            if idle:resources.append([side,*[round(statistics.median(r[k] for r in idle)/1024,2) for k in ['idle_rss_kib','connected_rss_kib','pinged_rss_kib']]])
        if resources:body+='<h3>2,000 connections: total process RSS</h3>'+table(['backend','before MiB','idle MiB','after PING MiB'],resources)
        ann=[]
        for side in ['baseline','current','dragonfly']:
            for r in selected:
                if r['core']!=core or r['side']!=side or not r['profile'].startswith('ann-'):continue
                for d in r.get('data',[]):
                    for v in d.get('latency',[]):ann.append([side,r['profile'],v['label'],rate(v['ops_per_sec']),f"{100*v.get('recall',0):.2f}%",round(v['p99_ns']/1e6,3)])
        if ann:body+='<h3>ANN quality alongside speed</h3><p>Default traversal encoding differs by backend. FLAT in the FyroDB baseline uses an Int8 shortlist and does not guarantee exact retrieval. Do not interpret QPS without recall. Vector memory accounting in the new version includes previously omitted auxiliary buffers.</p>'+table(['backend','algorithm','query setting','queries/s','recall@10','p99 ms'],ann)
        memory={}
        for r in selected:
            if r['core']==core and r['profile']=='memory':
                for d in r.get('data',[]):
                    for v in d.get('memory',[]):memory.setdefault((r['side'],v['scenario']),[]).append(v)
        mem=[[side,scenario,len(v),round(statistics.median(x['used_bytes_per_key'] for x in v),2),round(statistics.median(x['rss_bytes_per_key'] for x in v),2)] for (side,scenario),v in memory.items()]
        if mem:body+='<h3>Value-type memory: median across runs</h3><p>Per-key RSS figures are deltas and can reflect allocator reclamation. Total process RSS is the capacity measure.</p>'+table(['backend','scenario','runs','allocated bytes/key','RSS bytes/key'],mem)
        body+='</section>'
    body+='<p>PostgreSQL cache proxy results are FyroDB-only: Dragonfly does not implement that wire-protocol service, so it has no throughput score for it.</p>'
    skips=[]
    for r in rows:
        prefix=f"{r['side']}-{r['core']}-{r['config']}-{r['profile']}-{r['round']}-"
        for file in (root/r['dataset']).glob(prefix+'*.log'):
            for line in file.read_text(errors='replace').splitlines():
                if 'skipped' in line.lower():skips.append([r['side'],r['core'],r['profile'],line.strip()])
    if skips:body+='<h2>Explicitly skipped features</h2>'+table(['backend','CPU','profile','reason'],skips)
    if (root/'dragonfly-safety-selection.json').exists():
        safe=json.loads((root/'dragonfly-safety-selection.json').read_text());worker=json.loads((root/'worker-selection.json').read_text());values=[]
        for core,item in safe.items():
            if not isinstance(item,dict) or 'key_medians' not in item:continue
            fast=worker[core]['dragonfly']
            for name,metrics,note in [('fast multi-shard',fast['key_metrics'],'Snapshot invariant failed in the tested multicore setup'),('single data shard',item['key_medians'],'3 isolation rounds passed on this CPU setting')]:
                values.append([core,name,*[rate(metrics[k]) for k in ['Pipeline-64 SET','Pipelined SET','Pipelined GET']],note])
        body+='<h2>Dragonfly throughput versus validated isolation</h2><p>These are distinct configurations on the same CPU and RAM limits, not interchangeable scores. Results apply to the pinned v2.1 image and tested workload.</p>'+table(['CPU','preset','cold SET P64 ops/s','warm SET ops/s','GET ops/s','correctness'],values)
    body+='<h2>Validation and failures</h2><pre>'+html.escape(json.dumps(failures,indent=2))+'</pre>'
    isolation=[]
    for r in measured:
        for x in r.get('isolation',[]):isolation.append([r['side'],r['core'],x.get('mode',x.get('test','')),x.get('passed'),x.get('inconsistent',x.get('inconsistent_snapshots','')),x.get('errors'),x.get('final'),x.get('expected')])
    if isolation:body+=table(['backend','CPU','mode','passed','inconsistent snapshots','errors','final','expected'],isolation)
    for file,heading in [('state-results.json','SAVE/restart data checks'),('validation.json','Native regression checks'),('tuning-summary.json','Tuning candidates and acceptance'),('selection.json','Selected Docker options'),('interrupted-comparison.json','Agent-interrupted samples, excluded from measured results'),('worker-summary.json','KV-specific worker-count comparisons'),('worker-selection.json','KV-specific worker presets'),('worker-state-results.json','Worker-preset SAVE/restart validation'),('final-state-results.json','Final allocator-refined image SAVE/restart validation'),('dragonfly-safety-selection.json','Dragonfly correctness-compatible preset')]:
        if (root/file).exists():body+='<h2>'+heading+'</h2><pre>'+html.escape((root/file).read_text())+'</pre>'
    historical=[]
    for label,target in README.items():
        value=next((v['median'] for (c,s,_,m),v in stats.items() if c=='multi' and s=='current' and m==label),None)
        if value is not None:historical.append([label,rate(target),rate(value)])
    body+='<h2>README historical reference</h2><p>README rates are best-of-three using six physical cores; isolated multicore results here reserve two physical cores for clients. They are a historical reference, not a matched speedup claim. The maximum setting uses all allowed server CPUs and shares client cores, so it has different interference.</p>'+table(['workload','README rate','current isolated multi median'],historical)
    for directory,heading in [('worker-tuning','Worker-count tuning raw results'),('dragonfly-safety','Dragonfly transaction configuration checks')]:
        path=root/directory/'records.json'
        if path.exists():
            body+='<h2>'+heading+'</h2>'
            data=json.loads(path.read_text());values=[]
            for row in data:
                if row['round']<0:continue
                metrics=aggregate(row)
                values.append([row['side'],row['core'],row['config'],row['profile'],row['round'],row['status'],round(row['peak_rss_kib']/1024,2),'; '.join(k+': '+rate(v['ops_per_sec']) for k,v in metrics.items()),row.get('error','')])
            body+=table(['backend','CPU','preset','profile','round','status','peak RSS MiB','rates ops/s','detail'],values)
    body+='<h2>Exact images, resources, and client</h2><pre>'+html.escape(json.dumps(payload['datasets'],indent=2))+'</pre>'
    body+='<h2>Every run</h2>'+table(['backend','CPU','config','profile','round','status','wall seconds','peak process RSS MiB'],[[r['side'],r['core'],r['config'],r['profile'],r['round'],r['status'],round(r['wall_seconds'],2),round(r['peak_rss_kib']/1024,2)] for r in rows])
    body+='<p><a href="summary.json">Summary JSON</a> · <a href="throughput.csv">Throughput CSV</a> · <a href="comparison/records.json">Comparison raw samples</a> · <a href="tuning-verified/records.json">Tuning raw samples</a></p>'
    script="<script>document.querySelector('#core').onchange=e=>document.querySelectorAll('[data-core]').forEach(s=>s.hidden=e.target.value!=='all'&&s.dataset.core!==e.target.value);document.querySelectorAll('th').forEach(th=>th.onclick=()=>{let t=th.closest('table'),b=t.tBodies[0],i=th.cellIndex;Array.from(b.rows).sort((a,c)=>a.cells[i].textContent.localeCompare(c.cells[i].textContent,undefined,{numeric:true})).forEach(r=>b.appendChild(r));});</script>"
    css='body{font:14px system-ui,sans-serif;max-width:1600px;margin:32px auto;padding:0 24px;background:#f8fafc;color:#182635}table{border-collapse:collapse;width:100%;background:white;margin:12px 0 24px;font-variant-numeric:tabular-nums}td,th{padding:8px;border-bottom:1px solid #d6dfe7;text-align:right}td:first-child,td:nth-child(2),th:first-child,th:nth-child(2){text-align:left}th{background:#e5edf4;cursor:pointer}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf2f7;padding:16px}h2{margin-top:32px}p{line-height:1.6}a{color:#17618e}select{padding:8px}svg{width:100%;max-width:1040px;height:auto}section[hidden]{display:none}'
    (root/'report.html').write_text('<!doctype html><html><meta charset="utf-8"><title>'+title+'</title><style>'+css+'</style>'+body+script+'</html>')
    print(json.dumps(dict(measured=len(measured),failed=len(failures),targets=goals),indent=2))


if __name__=='__main__':
    main()
