# FyroDB documentation

This Next.js app renders the MDX pages in `src/content/`. From this directory:

```bash
bun install --frozen-lockfile
bun run dev
bun run build
bun run start
```

The benchmark page is `src/content/benchmarks.mdx`. It leads with the normal Go TCP suite, followed by independent standard clients, payload/memory comparisons and rejected experiments. `benchmark-history.mdx` preserves older best-of-three figures separately. `architecture.mdx` documents the current command isolation, map admission, entry guards and retained large-payload algorithms.

The versioned archive in `public/benchmarks/2026-11-13/` includes compressed raw records, image IDs, launch metadata, validation snapshots, summaries and a hash manifest. Its standalone `index.html` can be opened directly or served under `/benchmarks/2026-11-13/index.html`. No credentials or running server are needed to view it.

From the repository root, regenerate the normal-results table, root BENCHMARKS.md, download files and SVGs:

```bash
python3 scripts/update_benchmark_docs.py
python3 scripts/update_benchmark_docs.py --check
```

The generator uses the committed evidence and checks summary/raw-sample consistency. It never runs performance tests. Edit the benchmark MDX outside its generated table markers, then regenerate. Keep new campaigns separate, record immutable images and preserve slower, contended and failed attempts. Historical summaries do not automatically become current results.

Before committing docs, build the site and check affected links, figures and responsive layouts. Avoid publishing or deploying unless explicitly requested.
