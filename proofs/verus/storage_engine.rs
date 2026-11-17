// Formal Verus specification and proof for the storage engine:
// 1. Shard slot partitioning & hash boundary invariants.
// 2. TTL deadline evaluation & monotonicity under advancing clock.
// 3. Optimistic Concurrency Control (OCC) for WATCH/MULTI/EXEC transactions.

use vstd::prelude::*;

verus! {

// -------------------------------------------------------------
// Part 1: Storage Shard & Slot Invariants
// -------------------------------------------------------------

pub open spec fn shard_of(hash: nat, num_shards: nat) -> nat
    recommends num_shards > 0nat
{
    (hash % num_shards) as nat
}

pub proof fn shard_in_bounds(hash: nat, num_shards: nat)
    requires
        num_shards > 0nat,
    ensures
        shard_of(hash, num_shards) < num_shards,
{
}

// -------------------------------------------------------------
// Part 2: TTL Expiration Semantics & Monotonicity
// -------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ValueTTL {
    pub expires_ms: nat, // 0 means no expiration (persistent)
}

pub open spec fn is_expired(ttl: ValueTTL, now_ms: nat) -> bool {
    if ttl.expires_ms == 0nat {
        false
    } else {
        now_ms >= ttl.expires_ms
    }
}

pub proof fn ttl_monotonicity_theorem(ttl: ValueTTL, t1: nat, t2: nat)
    requires
        t1 <= t2,
        is_expired(ttl, t1),
    ensures
        is_expired(ttl, t2),
{
}

pub proof fn persistent_value_never_expires(ttl: ValueTTL, t: nat)
    requires
        ttl.expires_ms == 0nat,
    ensures
        !is_expired(ttl, t),
{
}

// -------------------------------------------------------------
// Part 3: WATCH / Optimistic Concurrency Control (OCC) Invariants
// -------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct WatchedState {
    pub key_version: nat,
    pub observed_version: nat,
    pub expires_ms: nat,
    pub write_occurred: bool,
}

pub open spec fn watch_inv(s: WatchedState) -> bool {
    // If a write occurred, key_version must strictly exceed observed_version
    s.write_occurred ==> (s.key_version > s.observed_version)
}

pub open spec fn can_commit(s: WatchedState, now_ms: nat) -> bool {
    let not_expired = s.expires_ms == 0nat || now_ms < s.expires_ms;
    s.key_version == s.observed_version && not_expired
}

pub proof fn occ_commit_soundness_theorem(s: WatchedState, now_ms: nat)
    requires
        watch_inv(s),
        can_commit(s, now_ms),
    ensures
        !s.write_occurred,
        s.expires_ms != 0nat ==> now_ms < s.expires_ms,
{
}

pub open spec fn step_write(s: WatchedState) -> WatchedState {
    WatchedState {
        key_version: (s.key_version + 1nat) as nat,
        observed_version: s.observed_version,
        expires_ms: s.expires_ms,
        write_occurred: true,
    }
}

pub proof fn step_write_preserves_watch_inv(s: WatchedState)
    requires
        watch_inv(s),
        s.key_version >= s.observed_version,
    ensures
        watch_inv(step_write(s)),
        !can_commit(step_write(s), 0nat),
{
}

}
