"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
   FiBarChart,
   FiBookOpen,
   FiBox,
   FiCode,
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
         { label: "v0.2.1", slug: "changelog-0-2-1", icon: FiFileText },
         { label: "v0.2.0", slug: "changelog-0-2-0", icon: FiFileText },
         { label: "v0.1.2", slug: "changelog-0-1-2", icon: FiFileText },
         { label: "v0.1.1", slug: "changelog-0-1-1", icon: FiFileText },
      ],
   },
];

export function Sidebar() {
   const pathname = usePathname();

   return (
      <aside className="hidden lg:block w-64 shrink-0 border-r border-border bg-sidebar-bg/70 overflow-y-auto h-[calc(100vh-4rem)] sticky top-16 p-5">
         {sections.map((section) => (
            <div key={section.title} className="mb-7">
               <p className="text-xs font-semibold uppercase tracking-wider text-muted mb-2">
                  {section.title}
               </p>
               <ul className="space-y-0.5">
                  {section.items.map((item) => {
                     const href = `/docs/${item.slug}`;
                     const active = pathname === href;
                     return (
                        <li key={item.slug}>
                           <Link
                              href={href}
                              className={`flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm transition-colors ${active ? "bg-primary/10 text-primary font-medium" : "text-muted hover:bg-card hover:text-fg"}`}
                           >
                              <item.icon size={15} />
                              {item.label}
                           </Link>
                        </li>
                     );
                  })}
               </ul>
            </div>
         ))}
      </aside>
   );
}
