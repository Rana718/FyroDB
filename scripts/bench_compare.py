#!/usr/bin/env python3
"""Diff two bench JSON files (bench -json out.json) and flag regressions.

    scripts/bench_compare.py baseline.json candidate.json [--threshold 5]

Throughput and recall regress when they drop; latency percentiles and
bytes/key regress when they grow. Exit status 1 when any metric regresses
by more than the threshold percent, so CI can gate on it.
"""
import argparse
import json
import sys


def index(rows, key="label"):
    return {r[key]: r for r in rows or []}


def pct(old, new):
    return (new - old) / old * 100 if old else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("baseline")
    ap.add_argument("candidate")
    ap.add_argument("--threshold", type=float, default=5.0)
    a = ap.parse_args()
    base = json.load(open(a.baseline))
    cand = json.load(open(a.candidate))
    regressions = 0

    def report(section, name, metric, old, new, higher_is_better):
        nonlocal regressions
        d = pct(old, new)
        bad = (d < -a.threshold) if higher_is_better else (d > a.threshold)
        mark = "REGRESSION" if bad else ("improved" if (d > a.threshold if higher_is_better else d < -a.threshold) else "")
        regressions += bad
        print(f"{section:<10} {name:<40} {metric:<14} {old:>14.2f} {new:>14.2f} {d:>+8.1f}%  {mark}")

    print(f"{'section':<10} {'name':<40} {'metric':<14} {'baseline':>14} {'candidate':>14} {'delta':>9}")
    bt, ct = index(base.get("throughput")), index(cand.get("throughput"))
    for k in bt.keys() & ct.keys():
        report("thr", k, "ops/sec", bt[k]["ops_per_sec"], ct[k]["ops_per_sec"], True)
    bl, cl = index(base.get("latency")), index(cand.get("latency"))
    for k in bl.keys() & cl.keys():
        report("lat", k, "ops/sec", bl[k]["ops_per_sec"], cl[k]["ops_per_sec"], True)
        for m in ("p50_ns", "p99_ns", "p999_ns"):
            report("lat", k, m, bl[k][m], cl[k][m], False)
        if bl[k].get("recall") and cl[k].get("recall"):
            report("lat", k, "recall", bl[k]["recall"], cl[k]["recall"], True)
    bm, cm = index(base.get("memory"), "scenario"), index(cand.get("memory"), "scenario")
    for k in bm.keys() & cm.keys():
        report("mem", k, "used B/key", bm[k]["used_bytes_per_key"], cm[k]["used_bytes_per_key"], False)
    sys.exit(1 if regressions else 0)


if __name__ == "__main__":
    main()
