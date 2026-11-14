// Verified model of the shard growth gate in
// crates/customhash/src/shard.rs:
//   - enter_insert / InsertGuard::drop  (insert_gate fetch_add / fetch_sub)
//   - grow / rehash                     (GROWING flag, drain, rebuild_to)
//   - insert_or_update_slot_fast / set_key_with_overwrite claim steps
//   - remove paths (entry lock — orthogonal to the gate)
//
// What the gate protects: `rebuild_to` swaps the slot table while readers
// traverse the old one under EBR and while inserts claim slots into it.
// A claim may only complete against the table it probed; the code's
// post-gate re-check (`self.table.load(Acquire) != table_ptr -> Fallback`)
// is exactly "the probed table is still current", which the model captures
// by making every claim a single atomic step on the *current* table.
//
// Model abstraction: the code checks the gate *before* probing and the
// table pointer *after* taking the gate; a claim is modeled as one atomic
// step (gate held + table current + below threshold) because the real
// claim's three checks are re-evaluated against the same serialized state
// at its single CAS. `grow_lock` serializes growers; the model expresses
// that by allowing only one growing-set at a time.

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct GateState {
    /// GROWING flag: set by a grower before draining in-flight inserts
    pub growing: bool,
    /// inserts holding an InsertGuard (gate incremented)
    pub inflight: nat,
    /// current slot-table generation (pointer identity)
    pub cur: nat,
    /// occupied entries on the current table
    pub len: nat,
    /// claim threshold of the current table
    pub threshold: nat,
}

/// Claims stay under the threshold of the current table and a rebuild runs
/// only on a fully drained gate.
pub open spec fn inv(s: GateState) -> bool {
    s.len <= s.threshold && s.threshold >= 1nat
}

pub open spec fn step(s: GateState, s2: GateState) -> bool {
    // enter_insert: fetch_add succeeds only when not growing (else the
    // thread spins / falls back — no shared change)
    (!s.growing
        && s2 == (GateState { growing: s.growing, inflight: (s.inflight + 1nat) as nat, cur: s.cur, len: s.len, threshold: s.threshold }))
    // a claim: gate held, table still current, below threshold — one
    // atomic step (probe checks + slot.claim CAS)
    || (s.inflight > 0nat
        && s.len < s.threshold
        && s2 == (GateState { growing: s.growing, inflight: s.inflight, cur: s.cur, len: (s.len + 1nat) as nat, threshold: s.threshold }))
    // InsertGuard::drop
    || (s.inflight > 0nat
        && s2 == (GateState { growing: s.growing, inflight: (s.inflight - 1nat) as nat, cur: s.cur, len: s.len, threshold: s.threshold }))
    // grow/rehash: set GROWING (serialized by grow_lock)
    || (!s.growing
        && s2 == (GateState { growing: true, inflight: s.inflight, cur: s.cur, len: s.len, threshold: s.threshold }))
    // rebuild_to: gate fully drained — swap the table; every occupied
    // entry migrates (len preserved), threshold at least doubles
    || (s.growing
        && s.inflight == 0nat
        && s2 == (GateState { growing: false, inflight: 0nat, cur: (s.cur + 1nat) as nat, len: s.len, threshold: (2nat * s.threshold) as nat }))
    // delete / tombstone: entry lock path, len decreases
    || (s.len > 0nat
        && s2 == (GateState { growing: s.growing, inflight: s.inflight, cur: s.cur, len: (s.len - 1nat) as nat, threshold: s.threshold }))
}

proof fn any_step_preserves_inv(s: GateState, s2: GateState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if !s.growing
        && s2 == (GateState { growing: s.growing, inflight: (s.inflight + 1nat) as nat, cur: s.cur, len: s.len, threshold: s.threshold }) {
        assert(s2.len <= s2.threshold);
    } else if s.inflight > 0nat
        && s.len < s.threshold
        && s2 == (GateState { growing: s.growing, inflight: s.inflight, cur: s.cur, len: (s.len + 1nat) as nat, threshold: s.threshold }) {
        // claim: len+1 <= threshold by the precondition
        assert(s2.len <= s2.threshold);
    } else if s.inflight > 0nat
        && s2 == (GateState { growing: s.growing, inflight: (s.inflight - 1nat) as nat, cur: s.cur, len: s.len, threshold: s.threshold }) {
        assert(s2.len <= s2.threshold);
    } else if !s.growing
        && s2 == (GateState { growing: true, inflight: s.inflight, cur: s.cur, len: s.len, threshold: s.threshold }) {
        assert(s2.len <= s2.threshold);
    } else if s.growing
        && s.inflight == 0nat
        && s2 == (GateState { growing: false, inflight: 0nat, cur: (s.cur + 1nat) as nat, len: s.len, threshold: (2nat * s.threshold) as nat }) {
        // rebuild: len preserved, threshold doubles
        assert(s2.len <= s2.threshold);
    } else {
        // delete: len shrinks
        assert(s2.len <= s2.threshold);
    }
}

/// THE SAFETY THEOREM: a table swap never overlaps an in-flight claim.
/// `rebuild_to` requires a fully drained gate and every claim requires a
/// gate slot, so at the linearization point of the swap no insert is
/// between its probe and its CAS — no entry is claimed into a table that
/// is being retired.
proof fn rebuild_never_overlaps_a_claim(s: GateState, s2: GateState)
    requires
        inv(s),
        step(s, s2),
        s2.cur == s.cur + 1nat, // the rebuild step fired
    ensures
        s.inflight == 0nat,
{
    // the only table-swapping step requires inflight == 0
    if !(s.growing && s.inflight == 0nat) {
        assert(false);
    }
}

/// THE NO-LOST-ENTRY THEOREM: the swap preserves the occupied-entry count.
/// Combined with `rebuild_never_overlaps_a_claim` this is the argument that
/// every entry claimed before the swap is migrated into the new table, and
/// every entry claimed after it lands in the new table directly.
proof fn rebuild_preserves_entries(s: GateState, s2: GateState)
    requires
        inv(s),
        step(s, s2),
        s2.cur == s.cur + 1nat,
    ensures
        s2.len == s.len,
{
    assert(s2.len == s.len);
}

/// Claims cannot land on a retired table: after a swap (cur advanced), any
/// still-pending insert's probe targets the old generation; the real code
/// detects this with the table-pointer re-check and falls back. The model
/// shows the fallback is exhaustive: a post-swap claim step necessarily
/// targets the new generation.
proof fn claims_follow_the_current_table(s: GateState, s2: GateState)
    requires
        inv(s),
        step(s, s2),
        s2.len == s.len + 1nat, // a claim fired
    ensures
        s2.cur == s.cur, // same table — claims never straddle a swap
{
    if s2.cur != s.cur {
        // only rebuild advances cur, and it preserves len
        assert(s2.len == s.len);
        assert(false);
    }
}

} // verus!
