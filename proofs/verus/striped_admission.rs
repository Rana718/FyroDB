// Verified model of multi-stripe writer admission and reader epoch isolation
// in crates/customhash/src/access.rs:
//   - active_writers counter (SeqCst)
//   - readers.count counter (SeqCst)
//   - Dekker handshake ensuring read epochs and write epochs are mutually exclusive.

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct StripedAdmState {
    /// Readers that executed fetch_add(1), awaiting verification
    pub pending_readers: nat,
    /// Readers that verified active_writers == 0 and hold ReadEpochGuard
    pub committed_readers: nat,
    /// Active writers that executed fetch_add(1) on active_writers
    pub active_writers: nat,
    /// A writer that observed readers.count == 0 and holds WriteEpochGuard
    pub writer_in_critical_section: bool,
}

/// Mutual exclusion invariant:
/// 1. Committed readers cannot exceed pending readers.
/// 2. If any reader is committed, no writer can be in its critical section.
/// 3. If a writer is in its critical section, active_writers must be > 0.
/// 4. If a writer is in its critical section, committed_readers must be 0.
pub open spec fn inv(s: StripedAdmState) -> bool {
    s.committed_readers <= s.pending_readers
        && (s.committed_readers == 0nat || !s.writer_in_critical_section)
        && (!s.writer_in_critical_section || s.active_writers > 0nat)
        && (s.writer_in_critical_section ==> s.committed_readers == 0nat)
}

pub open spec fn step(s: StripedAdmState, s2: StripedAdmState) -> bool {
    // 1. Reader begins registration: fetch_add(1) on readers.count
    (s2 == StripedAdmState {
        pending_readers: (s.pending_readers + 1nat) as nat,
        committed_readers: s.committed_readers,
        active_writers: s.active_writers,
        writer_in_critical_section: s.writer_in_critical_section,
    })
    // 2. Reader commits: observed active_writers == 0
    || (s.active_writers == 0nat
        && s.pending_readers > s.committed_readers
        && s2 == StripedAdmState {
            pending_readers: s.pending_readers,
            committed_readers: (s.committed_readers + 1nat) as nat,
            active_writers: s.active_writers,
            writer_in_critical_section: s.writer_in_critical_section,
        })
    // 3. Reader backs out: observed active_writers != 0, fetch_sub(1)
    || (s.pending_readers > s.committed_readers
        && s2 == StripedAdmState {
            pending_readers: (s.pending_readers - 1nat) as nat,
            committed_readers: s.committed_readers,
            active_writers: s.active_writers,
            writer_in_critical_section: s.writer_in_critical_section,
        })
    // 4. Committed reader drops: fetch_sub(1)
    || (s.committed_readers > 0nat
        && s2 == StripedAdmState {
            pending_readers: (s.pending_readers - 1nat) as nat,
            committed_readers: (s.committed_readers - 1nat) as nat,
            active_writers: s.active_writers,
            writer_in_critical_section: s.writer_in_critical_section,
        })
    // 5. Writer registers: fetch_add(1) on active_writers
    || (s2 == StripedAdmState {
        pending_readers: s.pending_readers,
        committed_readers: s.committed_readers,
        active_writers: (s.active_writers + 1nat) as nat,
        writer_in_critical_section: s.writer_in_critical_section,
    })
    // 6. Writer enters critical section: observed readers.count == 0 (so pending_readers == 0)
    || (s.active_writers > 0nat
        && !s.writer_in_critical_section
        && s.pending_readers == 0nat
        && s2 == StripedAdmState {
            pending_readers: s.pending_readers,
            committed_readers: s.committed_readers,
            active_writers: s.active_writers,
            writer_in_critical_section: true,
        })
    // 7. Writer exits critical section: drops guard, fetch_sub(1) on active_writers
    || (s.writer_in_critical_section
        && s.active_writers > 0nat
        && s2 == StripedAdmState {
            pending_readers: s.pending_readers,
            committed_readers: s.committed_readers,
            active_writers: (s.active_writers - 1nat) as nat,
            writer_in_critical_section: false,
        })
}

pub proof fn any_step_preserves_inv(s: StripedAdmState, s2: StripedAdmState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
}

/// Core Safety Theorem:
/// A committed reader holding a ReadEpoch and a writer in its critical section
/// CAN NEVER COEXIST simultaneously.
pub proof fn mutual_exclusion_theorem(s: StripedAdmState)
    requires
        inv(s),
    ensures
        !(s.committed_readers > 0nat && s.writer_in_critical_section),
{
}

} // verus!
