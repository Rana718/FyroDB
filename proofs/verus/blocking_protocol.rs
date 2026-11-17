// Verified model of the BlockRegistry waiter registration and wakeup protocol
// in src/storage/blocking.rs:
//   - waiting: AtomicUsize tracking total registered waiters
//   - per-key waiter lists behind shard mutexes
//   - signal: fast-path check against waiting counter, wake dispatch to waiters

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct BlockingState {
    /// Global atomic counter of registered waiters
    pub waiting_counter: nat,
    /// Waiters registered across all keys in the registry
    pub registered_waiters: nat,
    /// Total successful wakeup dispatches
    pub awakened_waiters: nat,
}

/// Soundness invariant:
/// The global waiting counter exactly tracks registered waiters.
pub open spec fn inv(s: BlockingState) -> bool {
    s.waiting_counter == s.registered_waiters
}

pub open spec fn step(s: BlockingState, s2: BlockingState) -> bool {
    // 1. Waiter registers: fetch_add(1) on waiting, adds entry to shard list
    (s2 == BlockingState {
        waiting_counter: (s.waiting_counter + 1nat) as nat,
        registered_waiters: (s.registered_waiters + 1nat) as nat,
        awakened_waiters: s.awakened_waiters,
    })
    // 2. Writer signals key when waiting_counter > 0: awakens a registered waiter
    || (s.waiting_counter > 0nat
        && s.registered_waiters > 0nat
        && s2 == BlockingState {
            waiting_counter: (s.waiting_counter - 1nat) as nat,
            registered_waiters: (s.registered_waiters - 1nat) as nat,
            awakened_waiters: (s.awakened_waiters + 1nat) as nat,
        })
    // 3. Writer signals when waiting_counter == 0: fast path returns immediately (no-op)
    || (s.waiting_counter == 0nat
        && s2 == s)
    // 4. Waiter times out: unregisters from shard, fetch_sub(1) on waiting
    || (s.waiting_counter > 0nat
        && s.registered_waiters > 0nat
        && s2 == BlockingState {
            waiting_counter: (s.waiting_counter - 1nat) as nat,
            registered_waiters: (s.registered_waiters - 1nat) as nat,
            awakened_waiters: s.awakened_waiters,
        })
}

pub proof fn any_step_preserves_inv(s: BlockingState, s2: BlockingState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
}

/// No-Lost-Wakeup Theorem:
/// When a write occurs and registered_waiters > 0, the waiting_counter is strictly positive,
/// so the writer fast-path check (waiting == 0) CANNOT skip signaling.
pub proof fn no_lost_wakeup_theorem(s: BlockingState)
    requires
        inv(s),
        s.registered_waiters > 0nat,
    ensures
        s.waiting_counter > 0nat,
{
}

} // verus!
