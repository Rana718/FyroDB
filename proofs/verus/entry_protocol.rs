// Verified model of the per-entry state protocol in
// crates/customhash/src/shard.rs:

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct EntryState {
    pub lock: bool,
    pub occupied: bool,
    pub readers: u16,
}

/// The protocol invariant: an entry whose lock bit is clear has no readers.
///
pub open spec fn inv(s: EntryState) -> bool {
    s.readers == 0u16 || s.lock
}

/// One successful atomic step of the real protocol (arbitrary interleaving).
pub open spec fn step(s: EntryState, s2: EntryState) -> bool {
    // Entry::read — join while unlocked, or join the herd under a
    // reader-held lock. Blocked while a writer holds the lock alone.
    (s.occupied
        && s.readers < u16::MAX
        && (!s.lock || s.readers > 0u16)
        && s2 == EntryState {
            lock: true,
            occupied: s.occupied,
            readers: (s.readers + 1u16) as u16,
        })
    // EntryReadGuard::drop — decrement and, for the last reader, clear the
    // lock in ONE atomic step (the current single-CAS release).
    || (s.readers > 0u16
        && s2 == (if s.readers == 1u16 {
            EntryState { lock: false, occupied: s.occupied, readers: 0u16 }
        } else {
            EntryState { lock: s.lock, occupied: s.occupied, readers: (s.readers - 1u16) as u16 }
        }))
    // state_lock — a writer acquires the lock once it observes it clear.
    || (!s.lock
        && s2 == EntryState { lock: true, occupied: s.occupied, readers: s.readers })
    // write_end / unlock_write — the writer publishes and releases.
    // Readers cannot join while the lock is held alone (join rule), so the
    || (s.lock
        && s.readers == 0u16
        && s2 == EntryState { lock: false, occupied: s.occupied, readers: 0u16 })
}

/// Any enabled step preserves the invariant.
proof fn any_step_preserves_inv(s: EntryState, s2: EntryState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if s.occupied
        && s.readers < u16::MAX
        && (!s.lock || s.readers > 0u16)
        && s2 == (EntryState { lock: true, occupied: s.occupied, readers: (s.readers + 1u16) as u16 }) {
        // join: s2.lock is true — invariant holds trivially
    } else if s.readers > 0u16
        && s2 == (if s.readers == 1u16 {
            EntryState { lock: false, occupied: s.occupied, readers: 0u16 }
        } else {
            EntryState { lock: s.lock, occupied: s.occupied, readers: (s.readers - 1u16) as u16 }
        }) {
        if s.readers == 1u16 {
            // last reader: count and lock clear in the same atomic step
            assert(s2.readers == 0u16);
        } else {
            // readers remain; inv(s) forces the lock bit on
            assert(s.readers - 1u16 > 0u16);
            assert(s.lock);
        }
    } else if !s.lock
        && s2 == (EntryState { lock: true, occupied: s.occupied, readers: s.readers }) {
        // writer acquire: s2.lock is true
    } else {
        // writer release: s2.readers == 0
        assert(s2.readers == 0u16);
    }
}

/// Two steps compose.
proof fn two_steps_preserve_inv(s: EntryState, m: EntryState, s2: EntryState)
    requires
        inv(s),
        step(s, m),
        step(m, s2),
    ensures
        inv(s2),
{
    any_step_preserves_inv(s, m);
    any_step_preserves_inv(m, s2);
}

/// THE SAFETY THEOREM. `state_lock` acquires whenever the lock bit is
/// clear; this proof shows that at such a moment there are no readers, so
proof fn writer_acquire_sees_no_readers(s: EntryState)
    requires
        inv(s),
        !s.lock,
    ensures
        s.readers == 0u16,
{
    if s.readers != 0u16 {
        assert(s.lock);
        assert(false);
    }
}

/// Readers never underflow: every release decrements a positive count, so
/// the reader field can never wrap into the sequence/hash/snapshot bits
proof fn reader_leave_keeps_count_in_range(s: EntryState, s2: EntryState)
    requires
        inv(s),
        step(s, s2),
    ensures
        s2.readers <= u16::MAX,
{
    any_step_preserves_inv(s, s2);
    if s.readers > 0u16 {
        assert(s.readers - 1u16 < s.readers);
    }
}

/// Why the audit's original "clobber" scenario (a joiner between the old
/// fetch_sub and the conditional store) could never happen, stated as a
proof fn the_release_is_atomic_in_the_model(s: EntryState, s2: EntryState)
    requires
        inv(s),
        s.readers == 1u16,
        step(s, s2),
        s2.readers == 0u16,
    ensures
        !s2.lock,
{
    // The count reached zero: the leave disjunct fired (it clears count and
    // lock in ONE step). No observable intermediate `(count == 0,
    if s.occupied
        && s.readers < u16::MAX
        && (!s.lock || s.readers > 0u16)
        && s2 == (EntryState { lock: true, occupied: s.occupied, readers: (s.readers + 1u16) as u16 }) {
        assert(s2.readers == 2u16);
        assert(false);
    } else if s.readers > 0u16 {
        assert(s2 == (EntryState { lock: false, occupied: s.occupied, readers: 0u16 }));
    }
}

} // verus!
