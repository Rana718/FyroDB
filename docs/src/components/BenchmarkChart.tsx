"use client";

import { useEffect, useRef, useState } from "react";

interface Bar {
   label: string;
   value: number;
   detail: string;
   /** Optional explicit palette key; falls back to per-label auto colors. */
   color?: string;
}

// Stable per-engine colors so a given engine keeps its hue across every chart,
// regardless of the order bars are sorted in.
const ENGINE_COLORS: Record<string, string> = {
   FyroDB: "var(--primary)",
   "Redis Cluster": "var(--accent-red)",
   DragonflyDB: "var(--accent-purple)",
   "DiceDB (1 node)": "var(--accent-yellow)",
};

function colorFor(bar: Bar): string {
   if (bar.color) return bar.color;
   return ENGINE_COLORS[bar.label] ?? "var(--muted)";
}

interface Chart {
   title: string;
   unit: string;
   /** Lower is better (e.g. RSS, CPU): the shortest bar wins the highlight. */
   lowerIsBetter?: boolean;
   bars: Bar[];
}

// Every benchmark from the suite gets a chart. `multi` charts compare engines;
// throughput values are in millions of ops/sec unless the unit says otherwise.
const CHARTS: Record<string, Chart> = {
   set: {
      title: "SET throughput · pipeline 100",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 20.08, detail: "20.08M" },
         { label: "Redis Cluster", value: 7.56, detail: "7.56M" },
         { label: "DragonflyDB", value: 4.16, detail: "4.16M" },
         { label: "DiceDB (1 node)", value: 1.62, detail: "1.62M" },
      ],
   },
   get: {
      title: "GET throughput · pipeline 100",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 28.16, detail: "28.16M" },
         { label: "Redis Cluster", value: 11.29, detail: "11.29M" },
         { label: "DragonflyDB", value: 4.09, detail: "4.09M" },
         { label: "DiceDB (1 node)", value: 1.88, detail: "1.88M" },
      ],
   },
   "pipeline64": {
      title: "Pipeline-64 SET · empty → 1M keys",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 8.95, detail: "8.95M" },
         { label: "Redis Cluster", value: 5.75, detail: "5.75M" },
         { label: "DragonflyDB", value: 4.09, detail: "4.09M" },
         { label: "DiceDB (1 node)", value: 0.766, detail: "766K" },
      ],
   },
   pubsub: {
      title: "Pub/Sub delivery throughput",
      unit: "M msg/sec",
      bars: [
         { label: "FyroDB", value: 74.64, detail: "74.64M" },
         { label: "DragonflyDB", value: 15.43, detail: "15.43M" },
         { label: "DiceDB (1 node)", value: 8.39, detail: "8.39M" },
         { label: "Redis Cluster", value: 7.35, detail: "7.35M" },
      ],
   },
   "pubsub-publish": {
      title: "Pub/Sub publish throughput",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 1.49, detail: "1.49M" },
         { label: "DragonflyDB", value: 0.3155, detail: "315.5K" },
         { label: "DiceDB (1 node)", value: 0.1678, detail: "167.8K" },
         { label: "Redis Cluster", value: 0.1471, detail: "147.1K" },
      ],
   },

   // ── Mixed / real-world workloads: FyroDB vs Redis Cluster vs DragonflyDB ──
   mixed: {
      title: "Mixed SET/GET (50/50)",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 20.9, detail: "20.90M" },
         { label: "Redis Cluster", value: 7.24, detail: "7.24M" },
         { label: "DragonflyDB", value: 5.15, detail: "5.15M" },
      ],
   },
   incr: {
      title: "INCR · atomic counters",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 50.11, detail: "50.11M" },
         { label: "DragonflyDB", value: 6.84, detail: "6.84M" },
         { label: "Redis Cluster", value: 6.32, detail: "6.32M" },
      ],
   },
   hash: {
      title: "HSET/HGET · sessions",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 33.07, detail: "33.07M" },
         { label: "Redis Cluster", value: 7.95, detail: "7.95M" },
         { label: "DragonflyDB", value: 5.81, detail: "5.81M" },
      ],
   },
   list: {
      title: "LPUSH/RPOP · queue",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 39.26, detail: "39.26M" },
         { label: "Redis Cluster", value: 7.19, detail: "7.19M" },
         { label: "DragonflyDB", value: 6.23, detail: "6.23M" },
      ],
   },
   sadd: {
      title: "SADD · per-client sets",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 27.87, detail: "27.87M" },
         { label: "Redis Cluster", value: 6.34, detail: "6.34M" },
         { label: "DragonflyDB", value: 5.0, detail: "5.00M" },
      ],
   },
   zadd: {
      title: "ZADD · per-client zsets",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 18.12, detail: "18.12M" },
         { label: "Redis Cluster", value: 4.55, detail: "4.55M" },
         { label: "DragonflyDB", value: 4.36, detail: "4.36M" },
      ],
   },
   json: {
      title: "JSON.SET/GET · documents",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 14.91, detail: "14.91M" },
         { label: "Redis Cluster", value: 3.38, detail: "3.38M" },
         { label: "DragonflyDB", value: 0.1515, detail: "151.5K" },
      ],
   },
   expire: {
      title: "SET+EXPIRE · cache TTL",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 10.51, detail: "10.51M" },
         { label: "Redis Cluster", value: 2.64, detail: "2.64M" },
         { label: "DragonflyDB", value: 2.41, detail: "2.41M" },
      ],
   },
   hotkey: {
      title: "Hot Key · 1 key, contention",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 44.07, detail: "44.07M" },
         { label: "DragonflyDB", value: 2.15, detail: "2.15M" },
         { label: "Redis Cluster", value: 2.03, detail: "2.03M" },
      ],
   },
   prodcons: {
      title: "Producer/Consumer · 50+50",
      unit: "M ops/sec",
      bars: [
         { label: "FyroDB", value: 36.2, detail: "36.20M" },
         { label: "DragonflyDB", value: 1.75, detail: "1.75M" },
         { label: "Redis Cluster", value: 0.9813, detail: "981.3K" },
      ],
   },

   // ── Resource usage: lower is better ──────────────────────────────────────
   "rss-peak": {
      title: "Peak RSS · lower is better",
      unit: "MB",
      lowerIsBetter: true,
      bars: [
         { label: "DragonflyDB", value: 235.8, detail: "235.80 MB" },
         { label: "FyroDB", value: 294.19, detail: "294.19 MB" },
         { label: "Redis Cluster", value: 767.63, detail: "767.63 MB" },
      ],
   },
   "cpu-avg": {
      title: "Avg CPU · lower is better",
      unit: "% of one core",
      lowerIsBetter: true,
      bars: [
         { label: "FyroDB", value: 42.5, detail: "42.5%" },
         { label: "Redis Cluster", value: 125.2, detail: "125.2%" },
         { label: "DragonflyDB", value: 430.6, detail: "430.6%" },
      ],
   },
};

// One shared observer-driven "in view" hook so bars only animate when scrolled to.
function useInView<T extends Element>(): [React.RefObject<T | null>, boolean] {
   const ref = useRef<T | null>(null);
   const [inView, setInView] = useState(false);

   useEffect(() => {
      const el = ref.current;
      if (!el) return;
      if (typeof IntersectionObserver === "undefined") {
         setInView(true);
         return;
      }
      const obs = new IntersectionObserver(
         (entries) => {
            for (const e of entries) {
               if (e.isIntersecting) {
                  setInView(true);
                  obs.disconnect();
                  break;
               }
            }
         },
         { threshold: 0.25 },
      );
      obs.observe(el);
      return () => obs.disconnect();
   }, []);

   return [ref, inView];
}

interface BenchmarkChartProps {
   id: string;
}

export function BenchmarkChart({ id }: BenchmarkChartProps) {
   const chart = CHARTS[id];
   const [ref, inView] = useInView<HTMLDivElement>();

   if (!chart) return null;

   const { title, unit, bars, lowerIsBetter } = chart;
   const max = Math.max(...bars.map((b) => b.value));
   const best = lowerIsBetter
      ? Math.min(...bars.map((b) => b.value))
      : Math.max(...bars.map((b) => b.value));

   const prefersReducedMotion =
      typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;

   return (
      <div
         ref={ref}
         className="my-6 rounded-xl border border-border bg-card/50 p-5"
      >
         {title && (
            <p className="mb-4 text-xs font-medium uppercase tracking-widest text-muted">
               {title}
            </p>
         )}
         <div className="space-y-3">
            {bars.map((bar, i) => {
               const pct = (bar.value / max) * 100;
               const color = colorFor(bar);
               const isWinner = bar.value === best;
               // Stagger each bar's reveal so the chart "draws" top to bottom.
               const delay = prefersReducedMotion ? 0 : i * 90;
               const width = inView || prefersReducedMotion ? `${pct}%` : "0%";

               return (
                  <div
                     key={bar.label}
                     className="grid grid-cols-[140px_1fr_82px] items-center gap-3"
                     style={{
                        opacity: inView || prefersReducedMotion ? 1 : 0,
                        transform:
                           inView || prefersReducedMotion
                              ? "translateY(0)"
                              : "translateY(6px)",
                        transition: `opacity 400ms ease ${delay}ms, transform 400ms ease ${delay}ms`,
                     }}
                  >
                     <span
                        className="truncate text-sm"
                        style={{
                           color: isWinner ? "var(--fg)" : "var(--muted)",
                           fontWeight: isWinner ? 600 : 400,
                        }}
                     >
                        {bar.label}
                     </span>
                     <div className="h-4 overflow-hidden rounded-full bg-code-bg">
                        <div
                           className="h-full rounded-full"
                           style={{
                              width,
                              background: color,
                              transition: `width 900ms cubic-bezier(0.22, 1, 0.36, 1) ${delay}ms`,
                           }}
                        />
                     </div>
                     <span
                        className="text-right font-mono text-xs"
                        style={{ color: isWinner ? color : "var(--muted)" }}
                     >
                        {bar.detail}
                     </span>
                  </div>
               );
            })}
         </div>
         <p className="mt-4 text-right text-xs text-muted">{unit}</p>
      </div>
   );
}
