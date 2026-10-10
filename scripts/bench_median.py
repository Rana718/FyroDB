#!/usr/bin/env python3
"""Median throughput per benchmark across run-<side>-<mode>-<i>.json files.

    scripts/bench_median.py /tmp/fyrodb-ab [--base-label 5fc1252]
"""
import argparse
import glob
import json
import os
import statistics


def collect(folder, side):
    rows = {}
    for path in sorted(glob.glob(os.path.join(folder, f"run-{side}-*.json"))):
        for r in json.load(open(path)).get("throughput") or []:
            rows.setdefault(r["label"], []).append(r["ops_per_sec"])
    return rows


def rate(v):
    for unit, div in (("M", 1e6), ("k", 1e3)):
        if v >= div:
            return f"{v / div:.2f}{unit}"
    return f"{v:.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--base-label", default="base")
    a = ap.parse_args()
    base, cand = collect(a.folder, "base"), collect(a.folder, "cand")
    print(f"{'benchmark':<34} {a.base_label:>12} {'candidate':>12} {'delta':>8}  runs")
    for label in dict.fromkeys(list(cand) + list(base)):
        b = statistics.median(base[label]) if label in base else 0
        c = statistics.median(cand[label]) if label in cand else 0
        d = (c - b) / b * 100 if b else 0
        n = len(cand.get(label, []))
        print(f"{label:<34} {rate(b):>12} {rate(c):>12} {d:>+7.1f}%  {n}")


if __name__ == "__main__":
    main()
