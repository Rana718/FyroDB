import Link from "next/link";
import { FiArrowUpRight } from "react-icons/fi";

// Normal Go TCP suite, November 16, 2026. Three-run medians in millions of operations per second.
const results = [
   { engine: "FyroDB", get: 24.39, set: 20.09 },
   { engine: "Dragonfly", get: 0.99, set: 0.83 },
   { engine: "Redis", get: 2.65, set: 1.54 },
];

export function EngineComparison() {
   return (
      <section id="benchmarks" className="engine-comparison" aria-labelledby="compare-title">
         <div className="comparison-intro">
            <h2 id="compare-title">Single-core Go benchmark</h2>
            <Link className="landing-text-link" href="/docs/benchmarks">Full benchmark results <FiArrowUpRight /></Link>
         </div>
         <table className="go-results">
            <caption>Million operations per second</caption>
            <thead><tr><th scope="col">Engine</th><th scope="col">GET</th><th scope="col">SET</th></tr></thead>
            <tbody>{results.map(({ engine, get, set }) => (
               <tr key={engine}><th scope="row">{engine}</th><td>{get.toFixed(2)}</td><td>{set.toFixed(2)}</td></tr>
            ))}</tbody>
         </table>
      </section>
   );
}
