"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { useEffect, useMemo, useState } from "react";
import {
   FiArrowUpRight,
   FiBarChart2,
   FiBook,
   FiMenu,
   FiMoon,
   FiSearch,
   FiSun,
   FiX,
   FiZap,
} from "react-icons/fi";
import { FaGithub } from "react-icons/fa";
import type { DocMeta } from "@/lib/docs";

const navLinks = [
   { href: "/docs/getting-started",    label: "Get started", icon: FiZap       },
   { href: "/docs/commands-strings",   label: "Commands",    icon: FiBook      },
   { href: "/docs/benchmarks",         label: "Benchmarks",  icon: FiBarChart2 },
   { href: "/docs/changelog-0-2-4",    label: "What's New",  icon: FiZap       },
];

function isLinkActive(href: string, pathname: string) {
   if (href === "/docs/commands-strings") return pathname.startsWith("/docs/commands");
   return pathname === href || pathname.startsWith(href);
}

export function Navbar({ docs }: { docs: DocMeta[] }) {
   const pathname = usePathname();
   const { theme, setTheme } = useTheme();
   const [mounted, setMounted] = useState(false);
   const [searchOpen, setSearchOpen] = useState(false);
   const [menuOpen, setMenuOpen] = useState(false);
   const [query, setQuery] = useState("");
   const [stars, setStars] = useState<string | null>(null);

   // Close mobile menu on route change
   useEffect(() => { setMenuOpen(false); }, [pathname]);

   const results = useMemo(() => {
      const value = query.trim().toLowerCase();
      if (!value) return docs;
      return docs.filter((doc) =>
         `${doc.title} ${doc.description} ${doc.slug}`
            .toLowerCase()
            .includes(value),
      );
   }, [docs, query]);

   useEffect(() => setMounted(true), []);
   useEffect(() => {
      const onKey = (event: KeyboardEvent) => {
         if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
            event.preventDefault();
            setSearchOpen((open) => !open);
         }
         if (event.key === "Escape") {
            setSearchOpen(false);
            setMenuOpen(false);
         }
      };
      window.addEventListener("keydown", onKey);
      fetch("https://api.github.com/repos/Rana718/FyroDB")
         .then((r) => (r.ok ? r.json() : null))
         .then(
            (data) =>
               data?.stargazers_count != null &&
               setStars(
                  new Intl.NumberFormat("en", { notation: "compact" }).format(
                     data.stargazers_count,
                  ),
               ),
         )
         .catch(() => undefined);
      return () => window.removeEventListener("keydown", onKey);
   }, []);

   const closeSearch = () => {
      setSearchOpen(false);
      setQuery("");
   };

   return (
      <>
         <header className="sticky top-0 z-50 border-b border-border/80 bg-bg/85 backdrop-blur-xl">
            <nav className="mx-auto flex h-16 max-w-[1500px] items-center gap-4 px-5 lg:px-8">

               {/* Logo */}
               <Link href="/" className="flex items-center gap-2.5 font-semibold text-fg">
                  <Image src="/logo.png" alt="FyroDB" width={34} height={34} className="object-contain" />
                  <span>Fyro<span className="text-primary">DB</span></span>
               </Link>

               {/* Desktop nav links */}
               <div className="hidden items-center gap-1 text-sm md:flex">
                  {navLinks.map(({ href, label, icon: Icon }) => {
                     const active = isLinkActive(href, pathname);
                     return (
                        <Link
                           key={href}
                           href={href}
                           className={[
                              "flex items-center gap-1.5 rounded-lg px-3 py-2 transition-colors",
                              active
                                 ? "bg-primary/10 font-medium text-primary"
                                 : "text-muted hover:bg-card hover:text-fg",
                           ].join(" ")}
                        >
                           <Icon size={14} />
                           {label}
                        </Link>
                     );
                  })}
               </div>

               {/* Right side */}
               <div className="ml-auto flex items-center gap-2">
                  {/* Search — desktop */}
                  <button
                     onClick={() => setSearchOpen(true)}
                     className="hidden h-9 items-center gap-2 rounded-lg border border-border bg-card/70 px-3 text-sm text-muted transition-colors hover:border-primary/50 hover:text-fg sm:flex"
                     aria-label="Search documentation"
                  >
                     <FiSearch size={14} />
                     <span>Search docs</span>
                     <kbd className="ml-3 rounded border border-border px-1.5 py-0.5 text-[10px]">⌘ K</kbd>
                  </button>

                  {/* Search icon — mobile */}
                  <button
                     onClick={() => setSearchOpen(true)}
                     className="rounded-lg p-2 text-muted transition-colors hover:bg-card hover:text-fg sm:hidden"
                     aria-label="Search documentation"
                  >
                     <FiSearch size={17} />
                  </button>

                  {/* GitHub */}
                  <a
                     href="https://github.com/Rana718/FyroDB"
                     target="_blank"
                     rel="noopener"
                     className="flex h-9 items-center gap-1.5 rounded-lg px-2 text-muted transition-colors hover:bg-card hover:text-fg"
                     aria-label="FyroDB on GitHub"
                  >
                     <FaGithub size={17} />
                     <span className="hidden text-xs sm:inline">
                        {stars ? `${stars} stars` : "GitHub"}
                     </span>
                     <FiArrowUpRight size={13} />
                  </a>

                  {/* Theme toggle */}
                  {mounted && (
                     <button
                        onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
                        className="rounded-lg p-2 text-muted transition-colors hover:bg-card hover:text-fg"
                        aria-label="Toggle theme"
                     >
                        {theme === "dark" ? <FiSun size={17} /> : <FiMoon size={17} />}
                     </button>
                  )}

                  {/* Hamburger — mobile only */}
                  <button
                     onClick={() => setMenuOpen((o) => !o)}
                     className="rounded-lg p-2 text-muted transition-colors hover:bg-card hover:text-fg md:hidden"
                     aria-label={menuOpen ? "Close menu" : "Open menu"}
                  >
                     {menuOpen ? <FiX size={20} /> : <FiMenu size={20} />}
                  </button>
               </div>
            </nav>

            {/* Mobile dropdown menu */}
            {menuOpen && (
               <div className="border-t border-border bg-bg/95 px-5 pb-4 pt-2 md:hidden">
                  <div className="flex flex-col gap-1">
                     {navLinks.map(({ href, label, icon: Icon }) => {
                        const active = isLinkActive(href, pathname);
                        return (
                           <Link
                              key={href}
                              href={href}
                              onClick={() => setMenuOpen(false)}
                              className={[
                                 "flex items-center gap-3 rounded-lg px-3 py-3 text-sm transition-colors",
                                 active
                                    ? "bg-primary/10 font-medium text-primary"
                                    : "text-muted hover:bg-card hover:text-fg",
                              ].join(" ")}
                           >
                              <Icon size={16} />
                              {label}
                           </Link>
                        );
                     })}
                  </div>
               </div>
            )}
         </header>

         {/* Search modal */}
         {searchOpen && (
            <div
               className="fixed inset-0 z-[60] bg-black/50 p-4 pt-[10vh] backdrop-blur-sm"
               onMouseDown={closeSearch}
            >
               <div
                  className="mx-auto max-w-2xl overflow-hidden rounded-xl border border-border bg-bg shadow-2xl"
                  onMouseDown={(e) => e.stopPropagation()}
               >
                  <div className="flex items-center gap-3 border-b border-border px-5 py-4">
                     <FiSearch className="text-muted" />
                     <input
                        autoFocus
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Search every documentation page..."
                        className="flex-1 bg-transparent text-base text-fg outline-none placeholder:text-muted"
                     />
                     <kbd className="rounded border border-border px-2 py-1 text-xs text-muted">ESC</kbd>
                  </div>
                  <div className="max-h-[55vh] overflow-y-auto p-2">
                     {results.length ? (
                        results.map((doc) => (
                           <Link
                              key={doc.slug}
                              href={`/docs/${doc.slug}`}
                              onClick={closeSearch}
                              className="group flex items-center justify-between gap-6 rounded-lg px-3 py-3 hover:bg-card"
                           >
                              <span>
                                 <span className="block text-sm font-medium text-fg group-hover:text-primary">
                                    {doc.title}
                                 </span>
                                 <span className="mt-0.5 block text-xs text-muted">
                                    {doc.description}
                                 </span>
                              </span>
                              <FiArrowUpRight className="shrink-0 text-muted" />
                           </Link>
                        ))
                     ) : (
                        <p className="px-3 py-8 text-center text-sm text-muted">
                           No documentation matched "{query}".
                        </p>
                     )}
                  </div>
               </div>
            </div>
         )}
      </>
   );
}
