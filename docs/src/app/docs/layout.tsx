import { Sidebar } from "@/components/Sidebar";

export default function DocsLayout({ children }: { children: React.ReactNode }) {
   return (
      <div className="docs-shell">
         <Sidebar />
         <main className="docs-main">{children}</main>
      </div>
   );
}
