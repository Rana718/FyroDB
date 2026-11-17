# Formal verification (Verus) — dev-only tooling

This directory holds **machine-checked proofs of the concurrency protocols**
used by the KV engine. The proofs are models of the real protocol code —
they verify with the [Verus](https://github.com/verus-lang/verus) SMT
checker and never enter the build, the binary, or any dependency graph
(no Cargo.toml here; the workspace ignores this directory).

## What is proven

### `entry_protocol.rs` — the per-entry state machine (`crates/customhash/src/shard.rs`)

Invariant: **an entry whose lock bit is clear has no readers.**
Consequences proven by the checker:

- `writer_acquire_sees_no_readers` — the safety theorem. `state_lock`
  acquires whenever `STATE_LOCK` is clear, and at that moment the reader
  count is zero, so in-place value mutation under the entry lock can never
  race a live `EntryReadGuard` dereference. This is what justifies the
  unguarded `assume_init_mut` / `overwrite_bytes` writes on the SET paths.
- Any interleaving of join / release / acquire / release preserves the
  invariant (`any_step_preserves_inv`), so it holds in every reachable state.
- The reader count can never underflow into the sequence/hash bits.
- The single-CAS release makes "count hits zero ⇒ lock is clear" one
  observable step (`the_release_is_atomic_in_the_model`).

### `admission_protocol.rs` — the map admission handshake (`crates/customhash/src/access.rs`)

Invariant: **a committed (admitted) read epoch never overlaps an active
writer mutation section.**

### `striped_admission.rs` — multi-stripe writer admission and reader epoch isolation

Invariant: **committed readers and writers in critical sections are strictly disjoint.**
Consequences:
- `mutual_exclusion_theorem` — `committed_readers > 0` and `writer_in_critical_section` can never coexist.
- Linearizable atomic count synchronization guarantees no ABA or lost-wake race conditions across arbitrary reader/writer interleavings.

### `blocking_protocol.rs` — BlockRegistry waiter registration and wakeup (`src/storage/blocking.rs`)

Invariant: **a write to a key with pending waiters is guaranteed to dispatch a wake.**
Consequences:
- `no_lost_wakeup_theorem` — when `registered_waiters > 0`, the fast-path check `waiting_counter == 0` cannot skip signaling, eliminating lost wakeups.

### `fence_protocol.rs` — the cluster write fence (`src/storage/store/fence.rs`)

Invariant: **while the barrier is held, no write section is open anywhere.**
`drain_writes` (slot migration, replica snapshot bootstrap) relies on this
for its quiescent-keyspace guarantee. Proven: writers cannot open tickets
while the barrier is held (`no_new_writes_while_holding`), and the hold
step's precondition is exactly the drained state.

### `gate_protocol.rs` — the shard growth gate (`crates/customhash/src/shard.rs`)

Invariants: **a table swap never overlaps an in-flight claim** and **the
occupied-entry count is preserved across the swap** (`len` invariance under
`rebuild_to`). Together: every entry claimed before the swap is migrated,
every entry claimed after it lands in the new table — the no-lost-entry
argument for the GROWING gate, proven rather than argued.

### `wake_protocol.rs` — the pub/sub arm/disarm wakeup (`src/pubsub/`)

Invariant (the safety core of no-lost-wakeup): **a non-empty queue is
always armed, the producer still owes its arm swap, or the consumer is in
its re-check window.** A producer arm that transitions the flag
false→true issues the wake; the consumer's disarm-then-recheck closes the
race with pushes that land while it is finishing. Full liveness requires
the standard fairness assumption (noted in the file).

### `seq_window.rs` — AOF sequence windows (`src/storage/persistence/aof.rs`)

Theorem: **two writers can never hand out the same sequence number.**
Every refill carves a disjoint W-wide range from the global counter (the
only global RMW), all handed-out numbers sit inside their writer's range,
and the ranges tile the sequence space without gaps or reuse — exactly
what the replay `merged()` ordering relies on. Window width is a spec
parameter (2 here, 64 in the code); the proofs are size-generic.

## Running the proofs

```sh
# one-time: install the Verus release binary (dev-only, outside the repo)
#   https://github.com/verus-lang/verus/releases — x86-linux zip
#   unzip to ~/tools/verus ; it wants the rustup toolchain named in its
#   error message (1.99.1 here — link the system one:
#   rustup toolchain link 1.99.1-x86_64-unknown-linux-gnu /usr )

task verify:proofs
# or directly:
~/tools/verus/verus --crate-type=lib proofs/verus/entry_protocol.rs
~/tools/verus/verus --crate-type=lib proofs/verus/admission_protocol.rs
```

Expected output: `verification results:: N verified, 0 errors`.

## Why models, not the raw source

Verus verifies a `spec`/`proof` model of the protocol; each transition in
the model mirrors one *successful* RMW of the real code, and failed CAS
attempts are retried from a fresh load (observationally no-ops). Hardware
linearizes RMWs on one `AtomicU64`, which is exactly the atomic-step
interleaving model used here. Faithfulness notes live at the top of each
file. The protocol bit-packing itself (which bits hold what) is covered by
the workspace unit tests, and the model fields map 1:1 onto the
`STATE_*` constants in `shard.rs`.

## Audit cross-reference

The first audit classified the old two-step reader release as a HIGH bug
(§6.1). While building these proofs the trace was re-derived formally and
the finding was **withdrawn**: the join rule (`lock set ⇒ join only when
readers > 0`) plus CAS base-matching plus RMW linearization make the old
two-step release safe as well — no joiner can succeed inside the window
because its CAS base no longer matches and fresh loads are blocked by the
join rule. The single-CAS release now in `shard.rs` is kept as defense in
depth: it makes the atomicity explicit in the code instead of relying on
the join rule. See `entry_protocol.rs` for the full argument and
`AUDIT.md` §6.1/§12.
