import Link from "next/link";
import { FiBookOpen, FiCode, FiDatabase, FiLayers, FiTerminal, FiZap } from "react-icons/fi";

const guides = [
   { href: "/docs/getting-started", title: "Get started", text: "Install FyroDB and run your first commands.", icon: FiZap },
   { href: "/docs/docker", title: "Run with Docker", text: "Start a container, connect a client, and persist data.", icon: FiTerminal },
   { href: "/docs/commands-strings", title: "Command reference", text: "Explore the Redis-compatible command set.", icon: FiCode },
   { href: "/docs/architecture", title: "Architecture", text: "Understand storage, concurrency, and persistence.", icon: FiLayers },
   { href: "/docs/benchmarks", title: "Benchmarks", text: "Explore recorded workloads and their methodology.", icon: FiDatabase },
   { href: "/docs/changelog-0-4-0", title: "What's new", text: "Read the latest changes in FyroDB 0.4.0.", icon: FiBookOpen },
];

export default function HomePage() {
   return (
      <main className="home-docs">
         <section className="home-hero">
            <p className="home-eyebrow">FYRODB DOCUMENTATION <span> / </span> v0.4.0</p>
            <h1>In-memory data,<br /><span>ready to build with.</span></h1>
            <p className="home-description">A Redis-compatible database built in Rust. Find the guides, commands, and architecture you need to get started.</p>
            <div className="home-actions"><Link className="home-button-primary" href="/docs/getting-started">Get started</Link><a className="home-button-secondary" href="https://github.com/Rana718/FyroDB">View on GitHub</a></div>
         </section>
         <section className="home-quickstart" aria-label="Quick start">
            <div className="home-code-header"><span>QUICK START</span><span>TERMINAL</span></div>
            <pre><code><span>$</span> docker run -p 8000:8000 rana718/fyrodb:latest{"\n"}<span>$</span> redis-cli -p 8000{"\n"}<span>›</span> SET hello world{"\n"}<b>OK</b>{"\n"}<span>›</span> GET hello{"\n"}<b>"world"</b></code></pre>
         </section>
         <section className="home-guides">
            <div className="home-section-heading"><div><p>EXPLORE</p><h2>Everything you need.</h2></div><p>From your first command to the details under the hood.</p></div>
            <div className="home-guide-grid">{guides.map(({ href, title, text, icon: Icon }) => <Link href={href} className="home-guide" key={href}><Icon size={19} aria-hidden="true"/><h3>{title}</h3><p>{text}</p></Link>)}</div>
         </section>
         <footer className="home-footer"><span>FyroDB · Built with Rust</span><a href="https://github.com/Rana718/FyroDB">GitHub</a></footer>
      </main>
   );
}
