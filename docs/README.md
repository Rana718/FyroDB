# FyroDB documentation

This Next.js app renders the MDX pages in `src/content/`. From this directory:

```bash
bun install --frozen-lockfile
bun run dev
bun run build
bun run start
```

The benchmark page is `src/content/benchmarks.mdx`. It leads with the normal Go TCP suite, followed by independent standard clients, payload/memory comparisons and rejected experiments. `benchmark-history.mdx` preserves older best-of-three figures separately. `architecture.mdx` documents the current command isolation, map admission, entry guards and retained large-payload algorithms.


Before committing docs, build the site and check affected links, figures and responsive layouts. Avoid publishing or deploying unless explicitly requested.
