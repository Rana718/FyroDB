"use client";

import { useEffect, useRef, useState } from "react";
import data from "@/lib/benchmark-data.json";

const workers = [1, 2, 3, 6, 12];
const workloads = data.filter((row) => row.unit === "M/s");

export function BenchmarkDashboard() {
   const [selected, setSelected] = useState("Pipelined GET");
   const menu = useRef<HTMLDetailsElement>(null);
   useEffect(() => {
      function closeOnOutsideClick(event: PointerEvent) {
         if (menu.current?.open && !menu.current.contains(event.target as Node)) {
            menu.current.open = false;
         }
      }
      document.addEventListener("pointerdown", closeOnOutsideClick);
      return () => document.removeEventListener("pointerdown", closeOnOutsideClick);
   }, []);
   const row = workloads.find((item) => item.label === selected) ?? workloads[0];
   const maximum = Math.max(...row.values);
   const memory = data.find((item) => item.label === "Peak RSS (MiB)");
   return (
      <section className="benchmark-dashboard" aria-label="Benchmark explorer">
         <div className="benchmark-intro"><span className="benchmark-kicker">Checkpoint d5a178b</span><h2>Throughput by worker count</h2><p>Explore the archived Go TCP measurements across five Docker CPU configurations on the Intel i5-11400H.</p></div>
         <div className="benchmark-meta"><span><strong>3 runs</strong> median</span><span><strong>5 bytes</strong> KV values</span><span><strong>Go client</strong> TCP suite</span></div>
         <div className="benchmark-panel">
            <div className="benchmark-panel-header"><div><p className="benchmark-overline">Throughput</p><h3>{row.label}</h3><p>{row.label === "Pub/Sub delivery" ? "Millions of recipient messages per second" : "Millions of operations per second"}</p></div><div className="benchmark-selector">
                  <span className="benchmark-selector-label">Workload</span>
                  <details className="benchmark-menu" ref={menu} onKeyDown={(event) => {
                     if (event.key === "Escape" && menu.current?.open) {
                        menu.current.open = false;
                        menu.current.querySelector("summary")?.focus();
                     }
                  }}>
                     <summary><span>{selected}</span><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true"><path d="m6 9 6 6 6-6" /></svg></summary>
                     <div className="benchmark-menu-options" aria-label="Choose workload">
                        {workloads.map((item) => <button type="button" key={item.label} className={item.label === selected ? "is-selected" : ""} aria-pressed={item.label === selected} onClick={() => {
                           setSelected(item.label);
                           if (menu.current) {
                              menu.current.open = false;
                              menu.current.querySelector("summary")?.focus();
                           }
                        }}><span>{item.label}</span>{item.label === selected && <span aria-hidden="true">✓</span>}</button>)}
                     </div>
                  </details>
               </div></div>
            <div className="benchmark-columns" role="img" aria-label={`${row.label}: ${row.values.map((value, index) => `${workers[index]} workers ${value} million ${row.label === "Pub/Sub delivery" ? "messages" : "operations"} per second`).join(", ")}`}>
               {row.values.map((value, index) => <div className="benchmark-column" key={workers[index]}><span className="benchmark-value">{value.toFixed(1)}</span><div className="benchmark-column-track"><div className={`benchmark-column-fill ${index === 4 ? "is-shared" : ""}`} style={{ height: `${Math.max(2, value / maximum * 100)}%` }} /></div><span className="benchmark-worker">{workers[index]}</span><span className="benchmark-worker-caption">{index === 4 ? "shared" : "workers"}</span></div>)}
            </div>
            <p className="benchmark-note">The 12-worker run shared CPUs with the client. The other configurations used separate client CPUs.</p>
         </div>
         {memory && <div className="benchmark-memory"><div><p className="benchmark-overline">Memory</p><h3>Peak server RSS</h3><p>Whole-suite peak, in MiB</p></div><div className="benchmark-memory-values">{memory.values.map((value, index) => <div key={workers[index]}><span>{workers[index]} workers{index === 4 ? " · shared" : ""}</span><strong>{value.toFixed(0)}</strong></div>)}</div></div>}
      </section>
   );
}
