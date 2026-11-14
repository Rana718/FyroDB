// Verified model of the cluster write fence in src/storage/store/fence.rs:
//   - begin_write        (worker write ticket: SeqCst counter step + engaged check)

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct FenceState {
    /// write tickets currently open (proceeded writers)
    pub in_flight: nat,
    /// engaged flag (SeqCst): set by the holder before draining
    pub engaged: bool,
    /// the holder drained every counter and holds the barrier
    pub holding: bool,
}

/// While the holder holds the barrier, no write section is open.
pub open spec fn inv(s: FenceState) -> bool {
    s.holding ==> (s.in_flight == 0nat && s.engaged)
}

pub open spec fn step(s: FenceState, s2: FenceState) -> bool {
    // begin_write: fetch_add(SeqCst) then the engaged load — linearized on
    // `engaged`. Enabled only while not engaged; a writer reaching the
    (!s.engaged
        && s2 == (FenceState { in_flight: (s.in_flight + 1nat) as nat, engaged: s.engaged, holding: s.holding }))
    // WriteTicket::drop: counter decrement (Release)
    || (s.in_flight > 0nat
        && s2 == (FenceState { in_flight: (s.in_flight - 1nat) as nat, engaged: s.engaged, holding: s.holding }))
    // cluster_write_guard: engaged.store(SeqCst) — before draining
    || (!s.engaged
        && s2 == (FenceState { in_flight: s.in_flight, engaged: true, holding: s.holding }))
    // the drain observed every counter at zero: the holder takes the
    // barrier (it already holds the gate mutex)
    || (s.engaged
        && s.in_flight == 0nat
        && s2 == (FenceState { in_flight: s.in_flight, engaged: true, holding: true }))
    // WriteFenceGuard::drop: disarm (Release store)
    || (s.holding
        && s2 == (FenceState { in_flight: s.in_flight, engaged: false, holding: false }))
}

proof fn any_step_preserves_inv(s: FenceState, s2: FenceState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if !s.engaged
        && s2 == (FenceState { in_flight: (s.in_flight + 1nat) as nat, engaged: s.engaged, holding: s.holding }) {
        // begin_write while not engaged: holding was false (holding ==> engaged)
        assert(!s.holding);
        assert(!s2.holding);
    } else if s.in_flight > 0nat
        && s2 == (FenceState { in_flight: (s.in_flight - 1nat) as nat, engaged: s.engaged, holding: s.holding }) {
        // ticket drop: counters only go down; holding unchanged
        assert(s2.holding == s.holding);
        if s2.holding {
            assert(s.holding);
            assert(s.in_flight == 0nat);
            assert(s.in_flight > 0nat);
            assert(false);
        }
    } else if !s.engaged
        && s2 == (FenceState { in_flight: s.in_flight, engaged: true, holding: s.holding }) {
        // engage: holding unchanged (and false — engaged was false)
        assert(!s.holding);
        assert(!s2.holding);
    } else if s.engaged
        && s.in_flight == 0nat
        && s2 == (FenceState { in_flight: s.in_flight, engaged: true, holding: true }) {
        // hold: precondition is exactly the invariant
        assert(s2.in_flight == 0nat);
    } else {
        // disarm: holding clears, invariant vacuous
        assert(!s2.holding);
    }
}

/// THE SAFETY THEOREM for `drain_writes` / slot migration: while the
/// barrier is held, no write section is open anywhere in the process, so
proof fn barrier_holds_keyspace_quiescent(s: FenceState)
    requires
        inv(s),
        s.holding,
    ensures
        s.in_flight == 0nat,
{
    assert(s.in_flight == 0nat);
}

/// Writers cannot open tickets while the barrier is held (begin_write
/// requires !engaged, and holding implies engaged).
proof fn no_new_writes_while_holding(s: FenceState, s2: FenceState)
    requires
        inv(s),
        step(s, s2),
        s.holding,
        s2.in_flight == s.in_flight + 1nat,
    ensures
        false,
{
    // the only counter-raising step requires !engaged; holding ==> engaged
    assert(s.engaged);
    assert(false);
}

} // verus!
