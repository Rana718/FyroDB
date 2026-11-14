// Verified model of the map-level read/write admission handshake in
// crates/customhash/src/access.rs (the counter pair guarding read epochs
// against concurrent writers).
//
// Faithfulness notes:
//  - The 16 writer stripes are aggregated into one `writers` count for the
//    correctness argument. Striping changes contention, not the protocol:
//    a reader's check inspects every stripe (`for stripe in writers`), and
//    a writer's registration persists in its stripe until its exit store,
//    so "any stripe nonzero" is exactly "some writer is registered".
//  - Each transition is one successful atomic RMW or load-decision. The
//    SeqCst orderings in access.rs provide the single total order this
//    interleaving model assumes (the Dekker pairing: the reader registers
//    then checks the writers; the writer registers then checks the readers;
//    whichever registers second observes the other).
//
// The invariant proven here is what the fast GET/SET paths rely on:
// `get_to_buf_epoch_entry_bytes` reads an entry *without* taking its
// per-entry read guard once a read epoch is admitted. That is only sound
// because an admitted (committed) read epoch can never overlap a writer's
// in-flight mutation section.

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct AdmState {
    /// threads registered as readers (fetch_add done), not yet decided
    pub pending: nat,
    /// threads past the writer check holding a read epoch
    pub committed: nat,
    /// registered writers (fetch_add done); an active writer keeps its
    /// registration until its exit
    pub writers: nat,
    /// a writer has drained all readers and is mutating the map
    pub writer_active: bool,
}

/// A committed read epoch never overlaps an active write section.
/// `(!writer_active || writers > 0)` mirrors the real protocol: a writer
/// registers before it waits/proceeds and keeps its stripe count until its
/// exit store, so an active writer is always visible to reader checks.
pub open spec fn inv(s: AdmState) -> bool {
    s.committed <= s.pending
        && (s.committed == 0nat || !s.writer_active)
        && (!s.writer_active || s.writers > 0nat)
}

// One successful atomic step of access.rs, split into named disjuncts so
// proofs can branch without struct literals in condition position.

pub open spec fn reg_reader(s: AdmState, s2: AdmState) -> bool {
    s2.pending == (s.pending + 1nat) as nat
        && s2.committed == s.committed
        && s2.writers == s.writers
        && s2.writer_active == s.writer_active
}

pub open spec fn commit_reader(s: AdmState, s2: AdmState) -> bool {
    // the per-stripe check passed: every writer stripe is zero. The
    // committing thread's own registration is still outstanding, so the
    // pending count strictly exceeds the committed count.
    s.writers == 0nat
        && s.pending > s.committed
        && s2 == (AdmState {
            pending: s.pending,
            committed: (s.committed + 1nat) as nat,
            writers: s.writers,
            writer_active: s.writer_active,
        })
}

pub open spec fn rollback_reader(s: AdmState, s2: AdmState) -> bool {
    // the check observed a registered writer: roll back the registration
    // (the rolling-back thread is itself still pending)
    s.writers > 0nat
        && s.pending > s.committed
        && s2 == (AdmState {
            pending: (s.pending - 1nat) as nat,
            committed: s.committed,
            writers: s.writers,
            writer_active: s.writer_active,
        })
}

pub open spec fn reg_writer(s: AdmState, s2: AdmState) -> bool {
    s2 == (AdmState {
        pending: s.pending,
        committed: s.committed,
        writers: (s.writers + 1nat) as nat,
        writer_active: s.writer_active,
    })
}

pub open spec fn writer_proceeds(s: AdmState, s2: AdmState) -> bool {
    // the writer registered first (fetch_add), then spun while
    // readers.count != 0 (the total registration count), then proceeded
    s.writers > 0nat
        && s.pending == 0nat
        && !s.writer_active
        && s2 == (AdmState {
            pending: s.pending,
            committed: s.committed,
            writers: s.writers,
            writer_active: true,
        })
}

pub open spec fn writer_exits(s: AdmState, s2: AdmState) -> bool {
    s.writer_active
        && s2 == (AdmState {
            pending: s.pending,
            committed: s.committed,
            writers: (s.writers - 1nat) as nat,
            writer_active: false,
        })
}

pub open spec fn epoch_exits(s: AdmState, s2: AdmState) -> bool {
    // ReadEpochGuard drop: committed epoch ends, registration drops
    s.committed > 0nat
        && s2 == (AdmState {
            pending: (s.pending - 1nat) as nat,
            committed: (s.committed - 1nat) as nat,
            writers: s.writers,
            writer_active: s.writer_active,
        })
}

pub open spec fn step(s: AdmState, s2: AdmState) -> bool {
    reg_reader(s, s2)
        || commit_reader(s, s2)
        || rollback_reader(s, s2)
        || reg_writer(s, s2)
        || writer_proceeds(s, s2)
        || writer_exits(s, s2)
        || epoch_exits(s, s2)
        || s2 == s // nested/other-map bookkeeping: no shared change
}

proof fn any_step_preserves_inv(s: AdmState, s2: AdmState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if reg_reader(s, s2) {
        // registration: committed and active untouched
        assert(s2.committed == s.committed);
        assert(s2.pending == (s.pending + 1nat) as nat);
    } else if commit_reader(s, s2) {
        // commit requires writers == 0, so no writer is active: an active
        // writer keeps its stripe registered until its exit
        assert(!s.writer_active);
        assert(s2.committed <= s2.pending);
        assert(s2.committed == 0nat || !s2.writer_active);
    } else if rollback_reader(s, s2) {
        assert(s2.committed == s.committed);
        assert(s2.pending == (s.pending - 1nat) as nat);
        assert(s2.committed <= s2.pending);
    } else if reg_writer(s, s2) {
        // writer registration: nothing in the invariant changes
    } else if writer_proceeds(s, s2) {
        // readers == 0 and committed <= pending gives committed == 0, so
        // the newly active writer starts clean
        assert(s.committed == 0nat);
        assert(s2.committed == 0nat);
        assert(s2.committed == 0nat || !s2.writer_active);
    } else if writer_exits(s, s2) {
        // writer exit: activity clears, the committed clause relaxes
        assert(!s2.writer_active);
    } else if epoch_exits(s, s2) {
        assert(s2.committed == (s.committed - 1nat) as nat);
        assert(s2.pending == (s.pending - 1nat) as nat);
        assert(s2.committed <= s2.pending);
    } else {
        // no-op bookkeeping step
    }
}

/// THE SAFETY THEOREM for the epoch-backed fast paths: while a writer is
/// mutating (registered + active), no reader holds a committed read epoch,
/// and a writer can only start after all committed epochs have drained.
/// Together these make the guard-free entry reads in
/// `get_to_buf_epoch_entry_bytes` sound.
proof fn committed_epochs_and_writers_exclude_each_other(s: AdmState)
    requires
        inv(s),
        s.writer_active,
    ensures
        s.committed == 0nat,
{
    assert(s.committed == 0nat || !s.writer_active);
}

/// A committing reader has observed every writer stripe as zero: because an
/// active writer stays registered until its exit, a commit can never
/// succeed inside a writer's window.
proof fn commit_requires_no_writers(s: AdmState, s2: AdmState)
    requires
        inv(s),
        step(s, s2),
        s2.committed == (s.committed + 1nat) as nat,
    ensures
        s.writers == 0nat && !s.writer_active,
{
    // The only step raising `committed` is commit_reader, which requires
    // writers == 0. Every other disjunct keeps committed equal, and a
    // no-op keeps it equal — contradicting the strict increase.
    if !commit_reader(s, s2) {
        assert(s2.committed == s.committed);
        assert(false);
    }
}

} // verus!
