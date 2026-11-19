import { MDXRemote } from "next-mdx-remote/rsc";
import remarkGfm from "remark-gfm";
import rehypePrettyCode from "rehype-pretty-code";
import { BenchmarkChart } from "./BenchmarkChart";
import { BenchmarkDashboard } from "./BenchmarkDashboard";

const components = {
   BenchmarkChart,
   BenchmarkDashboard,
};

export function MDXContent({ source }: { source: string }) {
   return (
      <div className="prose">
         <MDXRemote
            source={source}
            components={components}
            options={{
               mdxOptions: {
                  remarkPlugins: [remarkGfm],
                  rehypePlugins: [
                     [
                        rehypePrettyCode,
                        { theme: "github-dark-default", keepBackground: false },
                     ],
                  ],
               },
            }}
         />
      </div>
   );
}
