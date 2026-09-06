"use client";

interface Bar {
   label: string;
   value: number;
   detail: string;
}

// Color palette: index 0 = FyroDB (primary), rest = distinct accents
const COLORS = [
   "var(--primary)",  // FyroDB — blue
   "#ef4444",         // Redis Cluster — red
   "#8b5cf6",         // DragonflyDB — purple
   "#f59e0b",         // DiceDB — yellow
];

const CHARTS: Record<string, { title: string; unit: string; bars: Bar[] }> = {
   set: {
      title: "SET throughput · pipeline 100",
      unit: "ops/sec",
      bars: [
         { label: "FyroDB",          value: 20.08, detail: "20.08M" },
         { label: "Redis Cluster",   value: 7.56,  detail: "7.56M"  },
         { label: "DragonflyDB",     value: 3.78,  detail: "3.78M"  },
         { label: "DiceDB (1 node)", value: 1.62,  detail: "1.62M"  },
      ],
   },
   get: {
      title: "GET throughput · pipeline 100",
      unit: "ops/sec",
      bars: [
         { label: "FyroDB",          value: 28.16, detail: "28.16M" },
         { label: "Redis Cluster",   value: 11.29, detail: "11.29M" },
         { label: "DragonflyDB",     value: 3.97,  detail: "3.97M"  },
         { label: "DiceDB (1 node)", value: 1.88,  detail: "1.88M"  },
      ],
   },
   pubsub: {
      title: "Pub/Sub delivery throughput",
      unit: "msg/sec",
      bars: [
         { label: "FyroDB",          value: 74.64, detail: "74.64M" },
         { label: "DragonflyDB",     value: 12.78, detail: "12.78M" },
         { label: "DiceDB (1 node)", value: 8.39,  detail: "8.39M"  },
         { label: "Redis Cluster",   value: 7.35,  detail: "7.35M"  },
      ],
   },
};

interface BenchmarkChartProps {
   id: string;
}

export function BenchmarkChart({ id }: BenchmarkChartProps) {
   const chart = CHARTS[id];
   if (!chart) return null;

   const { title, unit, bars } = chart;
   const max = Math.max(...bars.map((b) => b.value));

   return (
      <div className="my-6 rounded-xl border border-border bg-card/50 p-5">
         {title && (
            <p className="mb-4 text-xs font-medium uppercase tracking-widest text-muted">
               {title}
            </p>
         )}
         <div className="space-y-3">
            {bars.map((bar, i) => {
               const pct = (bar.value / max) * 100;
               const color = COLORS[i % COLORS.length];
               const isFyro = i === 0;

               return (
                  <div
                     key={bar.label}
                     className="grid grid-cols-[140px_1fr_72px] items-center gap-3"
                  >
                     <span
                        className="truncate text-sm"
                        style={{
                           color: isFyro ? "var(--fg)" : "var(--muted)",
                           fontWeight: isFyro ? 600 : 400,
                        }}
                     >
                        {bar.label}
                     </span>
                     <div className="h-4 overflow-hidden rounded-full bg-code-bg">
                        <div
                           className="h-full rounded-full transition-all duration-500"
                           style={{ width: `${pct}%`, background: color }}
                        />
                     </div>
                     <span
                        className="text-right font-mono text-xs"
                        style={{ color: isFyro ? color : "var(--muted)" }}
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
