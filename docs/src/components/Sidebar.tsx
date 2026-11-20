"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
   FiBarChart,
   FiBookOpen,
   FiBox,
   FiCode,
   FiChevronRight,
   FiDatabase,
   FiFileText,
   FiSettings,
   FiTerminal,
   FiLayers,
   FiRadio,
   FiServer,
} from "react-icons/fi";
import { SiGo, SiNodedotjs, SiOpenjdk, SiPython, SiRust } from "react-icons/si";

const sections = [
   {
      title: "Getting Started",
      items: [
         { label: "Introduction", slug: "introduction", icon: FiBookOpen },
         { label: "Installation", slug: "getting-started", icon: FiBox },
         { label: "Docker", slug: "docker", icon: FiBox },
         { label: "Configuration", slug: "configuration", icon: FiSettings },
         { label: "Cluster", slug: "cluster", icon: FiServer },
      ],
   },
   {
      title: "Usage",
      items: [
         { label: "CLI & Redis Clients", slug: "cli-usage", icon: FiTerminal },
         { label: "Node.js (ioredis)", slug: "nodejs", icon: SiNodedotjs },
         { label: "Python (redis-py)", slug: "python", icon: SiPython },
         { label: "Go (go-redis)", slug: "golang", icon: SiGo },
         { label: "Rust (redis-rs)", slug: "rust-client", icon: SiRust },
         { label: "Java (Jedis)", slug: "java", icon: SiOpenjdk },
      ],
   },
   {
      title: "Commands",
      items: [
         { label: "Strings", slug: "commands-strings", icon: FiFileText },
         { label: "Keys", slug: "commands-keys", icon: FiLayers },
         { label: "Hashes", slug: "commands-hashes", icon: FiDatabase },
         { label: "Lists", slug: "commands-lists", icon: FiLayers },
         { label: "Sets", slug: "commands-sets", icon: FiLayers },
         { label: "Sorted Sets", slug: "commands-zsets", icon: FiBarChart },
         { label: "JSON", slug: "commands-json", icon: FiCode },
         { label: "Streams", slug: "commands-streams", icon: FiLayers },
         { label: "Pub/Sub", slug: "commands-pubsub", icon: FiRadio },
         { label: "Server", slug: "commands-server", icon: FiServer },
         { label: "Bitmap, HLL, Geo, Tx", slug: "commands-other", icon: FiBox },
      ],
   },
   {
      title: "Architecture",
      items: [
         { label: "Overview", slug: "architecture", icon: FiLayers },
         { label: "Persistence", slug: "persistence", icon: FiDatabase },
         { label: "Benchmarks", slug: "benchmarks", icon: FiBarChart },
         { label: "Production Tips", slug: "production-tips", icon: FiServer },
      ],
   },
   {
      title: "Changelog",
      items: [
         { label: "v0.5.0 — Latest", slug: "changelog-0-5-0", icon: FiFileText },
      ],
      history: [
         { label: "v0.4.0", slug: "changelog-0-4-0", icon: FiFileText },
         { label: "v0.3.1", slug: "changelog-0-3-1", icon: FiFileText },
         { label: "v0.3.0", slug: "changelog-0-3-0", icon: FiFileText },
         { label: "v0.2.4", slug: "changelog-0-2-4", icon: FiFileText },
         { label: "v0.2.3", slug: "changelog-0-2-3", icon: FiFileText },
         { label: "v0.2.2", slug: "changelog-0-2-2", icon: FiFileText },
         { label: "v0.2.1", slug: "changelog-0-2-1", icon: FiFileText },
         { label: "v0.2.0", slug: "changelog-0-2-0", icon: FiFileText },
         { label: "v0.1.2", slug: "changelog-0-1-2", icon: FiFileText },
         { label: "v0.1.1", slug: "changelog-0-1-1", icon: FiFileText },
      ],
   },
];

export function Sidebar() {
   const pathname = usePathname();

   function navItem(item: (typeof sections)[number]["items"][number]) {
      const href = `/docs/${item.slug}`;
      const active = pathname === href;
      return (
         <li key={item.slug}>
            <Link href={href} aria-current={active ? "page" : undefined} className={`docs-nav-link ${active ? "is-active" : ""}`}>
               <item.icon size={16} aria-hidden="true" />
               <span>{item.label}</span>
            </Link>
         </li>
      );
   }

   const navigation = (
      <nav aria-label="Documentation">
         {sections.map((section) => (
            <div key={section.title} className="docs-nav-section">
               <p className="docs-nav-heading">{section.title}</p>
               <ul>
                  {section.items.map(navItem)}
               </ul>
               {section.history && (
                  <details
                     key={pathname}
                     className="changelog-history"
                     open={section.history.some((item) => pathname === `/docs/${item.slug}`)}
                  >
                     <summary className="docs-nav-link"><FiChevronRight size={16} aria-hidden="true" />History</summary>
                     <ul>{section.history.map(navItem)}</ul>
                  </details>
               )}
            </div>
         ))}
      </nav>
   );

   return (
      <>
         <aside className="docs-sidebar">{navigation}</aside>
         <details className="docs-mobile-nav">
            <summary>Browse documentation</summary>
            {navigation}
         </details>
      </>
   );
}
