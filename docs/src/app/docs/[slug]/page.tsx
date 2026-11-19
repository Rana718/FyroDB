import { notFound } from "next/navigation";
import { getDoc, getAllSlugs } from "@/lib/docs";
import { PageOutline } from "@/components/PageOutline";
import { MDXContent } from "@/components/MDXContent";
import type { Metadata } from "next";

const BASE_URL = "https://fyrodb.vercel.app";

interface Props {
   params: Promise<{ slug: string }>;
}

export async function generateStaticParams() {
   return getAllSlugs().map((slug) => ({ slug }));
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
   const { slug } = await params;
   const doc = getDoc(slug);
   if (!doc) return {};

   const title = `${doc.title} | FyroDB Docs`;
   const description = doc.description || `FyroDB documentation — ${doc.title}`;
   const url = `${BASE_URL}/docs/${slug}`;

   return {
      title: doc.title,
      description,
      alternates: {
         canonical: `/docs/${slug}`,
      },
      openGraph: {
         type: "article",
         url,
         siteName: "FyroDB",
         title,
         description,
         images: [
            {
               url: `${BASE_URL}/logo.png`,
               width: 512,
               height: 512,
               alt: "FyroDB Logo",
            },
         ],
         locale: "en_US",
      },
      twitter: {
         card: "summary_large_image",
         title,
         description,
         images: [`${BASE_URL}/logo.png`],
      },
   };
}

export default async function DocPage({ params }: Props) {
   const { slug } = await params;
   const doc = getDoc(slug);
   if (!doc) notFound();

   return (
      <div className="docs-page-grid">
      <article className="docs-article">
         <header className="docs-page-header">
            <p className="docs-eyebrow">FyroDB documentation</p>
            <h1>{doc.title}</h1>
            {doc.description && <p className="docs-description">{doc.description}</p>}
         </header>
         <MDXContent source={doc.content} />
      </article>
      <PageOutline key={slug} />
      </div>
   );
}
