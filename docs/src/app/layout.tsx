import type { Metadata } from "next";
import { ThemeProvider } from "@/components/ThemeProvider";
import { Navbar } from "@/components/Navbar";
import { getAllDocs, getSearchRecords } from "@/lib/docs";
import "./globals.css";
import "@fontsource-variable/inter";
import "@fontsource-variable/jetbrains-mono";

const BASE_URL = "https://fyrodb.vercel.app";

const description = "FyroDB is a Redis-compatible in-memory database written in Rust. Explore setup guides, commands, and measured GET and SET throughput over TCP and in-process.";

export const metadata: Metadata = {
   title: {
      default: "FyroDB — Redis-compatible In-Memory Database in Rust",
      template: "%s | FyroDB Docs",
   },
   description,
   keywords: [
      "FyroDB",
      "Redis alternative",
      "Redis compatible",
      "Rust key-value store",
      "in-memory database",
      "RESP protocol",
      "key-value database Rust",
      "high performance database",
      "concurrent hashmap Rust",
      "Redis replacement",
      "in-memory cache Rust",
   ],
   metadataBase: new URL(BASE_URL),
   alternates: {
      canonical: "/",
   },
   robots: {
      index: true,
      follow: true,
      googleBot: {
         index: true,
         follow: true,
         "max-video-preview": -1,
         "max-image-preview": "large",
         "max-snippet": -1,
      },
   },
   openGraph: {
      type: "website",
      url: BASE_URL,
      siteName: "FyroDB",
      title: "FyroDB — Redis-compatible In-Memory Database in Rust",
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
      title: "FyroDB — Redis-compatible In-Memory Database in Rust",
      description,
      images: [`${BASE_URL}/logo.png`],
   },
   icons: {
      icon: "/logo.png",
      shortcut: "/logo.png",
      apple: "/logo.png",
   },
};

export default function RootLayout({
   children,
}: {
   children: React.ReactNode;
}) {
   const docs = getAllDocs();
   return (
      <html lang="en" suppressHydrationWarning className="font-sans">
         <body className="min-h-screen flex flex-col" suppressHydrationWarning>
            <ThemeProvider>
               <Navbar docs={docs} searchRecords={getSearchRecords()} />
               {children}
            </ThemeProvider>
         </body>
      </html>
   );
}
