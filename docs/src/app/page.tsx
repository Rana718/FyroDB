import Link from "next/link";
import type { IconType } from "react-icons";
import {
   FiArrowRight as ArrowRight,
   FiCheck as Check,
   FiCpu as Cpu,
   FiDatabase as Database,
   FiLock as Lock,
   FiRadio as Radio,
   FiShield as ShieldCheck,
   FiZap as Zap,
} from "react-icons/fi";
import { HiOutlineChartBar as Gauge } from "react-icons/hi2";

const benchmarks = [
   { name: "FyroDB", value: 20.08, color: "bg-primary", detail: "20.08M" },
   {
      name: "Redis Cluster",
      value: 7.56,
      color: "bg-accent-red",
      detail: "7.56M",
   },
   {
      name: "DragonflyDB",
      value: 3.78,
      color: "bg-accent-blue",
      detail: "3.78M",
   },
   { name: "DiceDB (1 node)", value: 1.62, color: "bg-accent-yellow", detail: "1.62M" },
];

function BenchmarkRows() {
   return (
      <div className="space-y-5">
         {benchmarks.map((item) => (
            <div
               key={item.name}
               className="grid grid-cols-[108px_1fr_58px] items-center gap-3 text-sm"
            >
               <span
                  className={
                     item.name === "FyroDB"
                        ? "font-semibold text-fg"
                        : "text-muted"
                  }
               >
                  {item.name}
               </span>
               <div className="h-3 overflow-hidden rounded-full bg-code-bg">
                  <div
                     className={`h-full rounded-full ${item.color}`}
                     style={{ width: `${(item.value / 20.08) * 100}%` }}
                  />
               </div>
               <span className="text-right font-mono text-xs text-muted">
                  {item.detail}
               </span>
            </div>
         ))}
      </div>
   );
}
function Feature({
   icon: Icon,
   title,
   text,
}: {
   icon: IconType;
   title: string;
   text: string;
}) {
   return (
      <div className="border-t border-border pt-5">
         <Icon size={19} className="text-primary" />
         <h3 className="mt-4 font-semibold text-fg">{title}</h3>
         <p className="mt-2 text-sm leading-6 text-muted">{text}</p>
      </div>
   );
}

export default function HomePage() {
   return (
      <main className="overflow-hidden">
         <section className="mx-auto grid max-w-7xl gap-14 px-5 pb-20 pt-20 lg:grid-cols-[1.1fr_.9fr] lg:items-center lg:px-8 lg:pb-28 lg:pt-28">
            <div>
               <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-primary/25 bg-primary/10 px-3 py-1.5 text-xs font-medium text-primary">
                  <span className="h-1.5 w-1.5 rounded-full bg-accent-green" />
                  v0.2.1 · Open source · Rust-powered
               </div>
               <h1 className="max-w-3xl text-5xl font-semibold tracking-[-0.04em] text-fg sm:text-7xl">
                  The fast path to{" "}
                  <span className="text-primary">in-memory data.</span>
               </h1>
               <p className="mt-6 max-w-xl text-lg leading-8 text-muted">
                  FyroDB is a Redis-compatible, lock-free key-value store built
                  in Rust. More throughput, less machinery, and a familiar
                  protocol.
               </p>
               <div className="mt-9 flex flex-wrap items-center gap-3">
                  <Link
                     href="/docs/getting-started"
                     className="inline-flex items-center gap-2 rounded-lg bg-primary px-5 py-3 text-sm font-semibold text-primary-fg shadow-lg shadow-primary/20 transition-transform hover:-translate-y-0.5"
                  >
                     Start building <ArrowRight size={16} />
                  </Link>
                  <Link
                     href="/docs/benchmarks"
                     className="inline-flex items-center gap-2 rounded-lg border border-border px-5 py-3 text-sm font-medium text-fg hover:bg-card"
                  >
                     See the benchmarks
                  </Link>
               </div>
               <div className="mt-10 flex flex-wrap gap-x-6 gap-y-2 text-xs text-muted">
                  <span className="inline-flex items-center gap-1.5">
                     <Check size={14} className="text-accent-green" />
                     Redis clients work out of the box
                  </span>
                  <span className="inline-flex items-center gap-1.5">
                     <Check size={14} className="text-accent-green" />
                     Single-node simplicity
                  </span>
               </div>
            </div>
            <div className="relative rounded-2xl border border-border bg-card/60 p-6 shadow-2xl shadow-primary/5">
               <div className="mb-7 flex items-center justify-between border-b border-border pb-4">
                  <div>
                     <p className="text-xs font-medium uppercase tracking-[.18em] text-muted">
                        Live benchmark
                     </p>
                     <p className="mt-1 text-sm text-fg">
                        SET throughput · pipeline 100
                     </p>
                  </div>
                  <Gauge size={20} className="text-primary" />
               </div>
               <div className="mb-8 flex items-end gap-3">
                  <span className="text-6xl font-semibold tracking-tight text-fg">
                     20.08
                  </span>
                  <span className="mb-2 font-mono text-lg text-primary">
                     M ops/s
                  </span>
               </div>
               <BenchmarkRows />
               <div className="mt-7 flex items-center justify-between border-t border-border pt-4 text-xs text-muted">
                  <span>100 clients · 1M operations</span>
                  <Link
                     href="/docs/benchmarks"
                     className="text-primary hover:underline"
                  >
                     Methodology{" "}
                     <ArrowRight size={12} className="ml-1 inline" />
                  </Link>
               </div>
            </div>
         </section>
         <section className="border-y border-border bg-card/30">
            <div className="mx-auto grid max-w-7xl grid-cols-2 divide-x divide-border lg:grid-cols-4">
               <div className="p-6 lg:p-8">
                  <p className="text-3xl font-semibold text-fg">28.16M</p>
                  <p className="mt-1 text-xs text-muted">GET ops/sec</p>
               </div>
               <div className="p-6 lg:p-8">
                  <p className="text-3xl font-semibold text-fg">294.19 MB</p>
                  <p className="mt-1 text-xs text-muted">peak RSS</p>
               </div>
               <div className="p-6 lg:p-8">
                  <p className="text-3xl font-semibold text-fg">50.11M</p>
                  <p className="mt-1 text-xs text-muted">INCR ops/sec</p>
               </div>
               <div className="p-6 lg:p-8">
                  <p className="text-3xl font-semibold text-fg">39.26M</p>
                  <p className="mt-1 text-xs text-muted">LPUSH/RPOP ops/sec</p>
               </div>
            </div>
         </section>
         <section className="mx-auto max-w-7xl px-5 py-20 lg:px-8 lg:py-28">
            <div className="max-w-2xl">
               <p className="text-sm font-medium text-primary">
                  Built for real workloads
               </p>
               <h2 className="mt-3 text-3xl font-semibold tracking-tight text-fg sm:text-4xl">
                  A focused engine with fewer tradeoffs.
               </h2>
            </div>
            <div className="mt-14 grid gap-x-10 gap-y-12 sm:grid-cols-2 lg:grid-cols-3">
               <Feature
                  icon={Lock}
                  title="Lock-free by design"
                  text="Concurrent reads and writes without a mutex on the hot path, using atomic snapshots and epoch reclamation."
               />
               <Feature
                  icon={Zap}
                  title="Zero-copy reads"
                  text="Values write directly to the network buffer, avoiding clones and allocations on GET."
               />
               <Feature
                  icon={Cpu}
                  title="Thread-per-core"
                  text="One event loop per core with SO_REUSEPORT for predictable connection distribution."
               />
               <Feature
                  icon={Database}
                  title="Redis compatible"
                  text="Use redis-cli, ioredis, redis-py, go-redis, or any RESP-compatible client."
               />
               <Feature
                  icon={Radio}
                  title="Fast Pub/Sub"
                  text="Lock-free fan-out delivers 74.64M messages per second in the published benchmark."
               />
               <Feature
                  icon={ShieldCheck}
                  title="Crash-safe persistence"
                  text="Automatic RDB snapshots, atomic writes, and a straightforward operational model."
               />
            </div>
         </section>
         <section className="mx-auto max-w-7xl px-5 pb-24 lg:px-8">
            <div className="grid gap-8 rounded-2xl border border-border bg-card/50 p-6 sm:p-10 lg:grid-cols-[1fr_1.1fr] lg:items-center">
               <div>
                  <p className="text-sm font-medium text-primary">
                     Five minutes to first command
                  </p>
                  <h2 className="mt-3 text-3xl font-semibold tracking-tight text-fg">
                     Drop FyroDB into your stack.
                  </h2>
                  <p className="mt-4 max-w-md leading-7 text-muted">
                     Start a server, connect with your existing Redis tooling,
                     and keep moving.
                  </p>
                  <Link
                     href="/docs/getting-started"
                     className="mt-7 inline-flex items-center gap-2 text-sm font-semibold text-primary hover:underline"
                  >
                     Read the quickstart <ArrowRight size={15} />
                  </Link>
               </div>
               <pre className="overflow-x-auto rounded-xl border border-border bg-code-bg p-5 text-sm leading-7 text-fg">
                  <code>
                     <span className="text-muted">$</span> docker run -p
                     8000:8000 rana718/fyrodb:latest{"\n"}
                     <span className="text-muted">$</span> redis-cli -p 8000
                     {"\n"}
                     <span className="text-accent-blue">
                        127.0.0.1:8000&gt;
                     </span>{" "}
                     SET hello world{"\n"}
                     <span className="text-accent-green">OK</span>
                     {"\n"}
                     <span className="text-accent-blue">
                        127.0.0.1:8000&gt;
                     </span>{" "}
                     GET hello{"\n"}
                     <span className="text-primary">"world"</span>
                  </code>
               </pre>
            </div>
         </section>
         <footer className="border-t border-border px-5 py-8 text-center text-xs text-muted">
            Built on a 6-core Intel i5-11400H ·{" "}
            <Link
               href="/docs/benchmarks"
               className="text-primary hover:underline"
            >
               Full benchmark details
            </Link>{" "}
            ·{" "}
            <a
               href="https://github.com/Rana718/FyroDB"
               target="_blank"
               rel="noopener"
               className="text-primary hover:underline"
            >
               Star FyroDB on GitHub
            </a>
         </footer>
      </main>
   );
}
