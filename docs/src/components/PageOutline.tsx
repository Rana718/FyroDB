"use client";

import { useEffect, useState } from "react";
import { FiAlignLeft } from "react-icons/fi";

type Heading = { id: string; text: string; level: number };

export function PageOutline() {
   const [headings, setHeadings] = useState<Heading[]>([]);
   const [active, setActive] = useState("");

   useEffect(() => {
      const elements = Array.from(document.querySelectorAll<HTMLElement>(".docs-article .prose h2, .docs-article .prose h3, .docs-article .prose h4")).filter((element) => !element.closest(".benchmark-dashboard"));
      const used = new Map<string, number>();
      const items = elements.map((element) => {
         const text = element.textContent?.trim() ?? "";
         const base = text.toLowerCase().replace(/[^\p{L}\p{N}\s-]/gu, "").trim().replace(/\s+/g, "-") || "section";
         const count = used.get(base) ?? 0;
         used.set(base, count + 1);
         const id = element.id || (count ? `${base}-${count}` : base);
         element.id = id;
         return { id, text, level: Number(element.tagName[1]) };
      }).filter((item) => item.text);
      setHeadings(items);
      if (window.location.hash) {
         const target = document.getElementById(decodeURIComponent(window.location.hash.slice(1)));
         if (target) {
            let parent = target.parentElement;
            while (parent) {
               if (parent instanceof HTMLDetailsElement) parent.open = true;
               parent = parent.parentElement;
            }
            target.scrollIntoView();
         }
      }
      const update = () => {
         const visible = elements.filter((element) => element.getClientRects().length > 0);
         const current = visible.filter((element) => element.getBoundingClientRect().top <= 140).at(-1) ?? visible[0];
         setActive(current?.id ?? "");
      };
      update();
      window.addEventListener("scroll", update, { passive: true });
      window.addEventListener("resize", update);
      return () => { window.removeEventListener("scroll", update); window.removeEventListener("resize", update); };
   }, []);

   if (!headings.length) return null;
   return (
      <aside className="page-outline" aria-label="On this page">
         <details className="page-outline-mobile">
            <summary><FiAlignLeft aria-hidden="true" /> On this page</summary>
            <OutlineLinks headings={headings} active={active} />
         </details>
         <div className="page-outline-desktop">
            <p><FiAlignLeft aria-hidden="true" /> On this page</p>
            <OutlineLinks headings={headings} active={active} />
         </div>
      </aside>
   );
}

function OutlineLinks({ headings, active }: { headings: Heading[]; active: string }) {
   return <nav><ul>{headings.map((heading, index) => (
      <li key={heading.id} className={`outline-level-${heading.level} ${index > 0 && headings[index - 1].level !== heading.level ? "outline-transition" : ""}`}>
         <a href={`#${heading.id}`} aria-current={active === heading.id ? "location" : undefined} onClick={() => {
            const element = document.getElementById(heading.id);
            let parent = element?.parentElement;
            while (parent) {
               if (parent instanceof HTMLDetailsElement) parent.open = true;
               parent = parent.parentElement;
            }
         }}>{heading.text}</a>
      </li>
   ))}</ul></nav>;
}
