import Image from "next/image";
import Link from "next/link";
import { FiArrowRight, FiArrowUpRight, FiBox, FiCode, FiGithub, FiLayers, FiTerminal } from "react-icons/fi";
import { EngineComparison } from "@/components/EngineComparison";
import "./landing.css";

const guides = [
   { href: "/docs/getting-started", title: "Getting started", text: "Install FyroDB and connect your first client.", icon: FiTerminal },
   { href: "/docs/docker", title: "Docker deployment", text: "Run a container and persist your data.", icon: FiBox },
   { href: "/docs/commands-strings", title: "Command reference", text: "Look up commands and their arguments.", icon: FiCode },
   { href: "/docs/architecture", title: "Architecture", text: "Explore storage, concurrency, and isolation.", icon: FiLayers },
];

export default function HomePage() {
   return (
      <main className="landing">
         <div className="landing-mast">
            <section className="landing-hero landing-wrap">
               <div className="landing-lead">
                  <Link className="landing-version" href="/docs/changelog-0-5-0"><i aria-hidden="true" />FyroDB v0.5.0 <FiArrowUpRight /></Link>
                  <h1>In-memory database<br /><span>built in Rust.</span></h1>
                  <p>Use your Redis client to store key-value data, run transactions, and search vectors with FyroDB.</p>
                  <div className="landing-actions">
                     <Link className="landing-primary" href="/docs/getting-started">Get started <FiArrowRight /></Link>
                     <Link className="landing-secondary" href="#benchmarks">Go benchmarks</Link>
                  </div>
               </div>
               <Image className="landing-illustration" src="/engine-layers.svg" width={560} height={410} alt="" priority />
            </section>
         </div>
         <div className="landing-wrap">
            <EngineComparison />
            <section className="landing-native" aria-labelledby="native-title">
               <h2 id="native-title">FyroDB with 12 workers</h2>
               <dl>
                  <div><dt>Pipelined GET / sec</dt><dd>49.75<span>M</span></dd></div>
                  <div><dt>Pipelined SET / sec</dt><dd>44.27<span>M</span></dd></div>
                  <div><dt>Peak server RSS</dt><dd>1,096<span> MiB</span></dd></div>
               </dl>
            </section>
            <div className="landing-resources">
               <section className="landing-start" aria-labelledby="start-title">
                  <h2 id="start-title">Run locally</h2>
                  <div className="landing-terminal">
                     <div><FiTerminal aria-hidden="true" /> Terminal</div>
                     <pre tabIndex={0} aria-label="Docker quick start commands"><code><span>$</span> docker run -d -p 127.0.0.1:8000:8000 rana718/fyrodb:latest{"\n"}<span>$</span> redis-cli -p 8000 SET hello world{"\n"}<b>OK</b>{"\n"}<span>$</span> redis-cli -p 8000 GET hello{"\n"}<b>"world"</b></code></pre>
                  </div>
               </section>
               <section className="landing-guides" aria-label="Documentation guides">
                  {guides.map(({ href, title, text, icon: Icon }) => <Link key={href} href={href}><Icon aria-hidden="true" /><div><h3>{title}</h3><p>{text}</p></div><FiArrowUpRight aria-hidden="true" /></Link>)}
               </section>
            </div>
            <footer className="landing-footer"><span>FyroDB</span><a className="landing-text-link" href="https://github.com/Rana718/FyroDB"><FiGithub /> GitHub</a></footer>
         </div>
      </main>
   );
}
