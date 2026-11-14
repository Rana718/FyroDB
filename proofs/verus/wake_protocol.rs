// Verified model of the pub/sub wakeup protocol in src/pubsub/ (signal.rs
// arm/disarm over the worker queues: notify / notify_fanout producers,
// drain_*_limit consumers).
//
//   producer:  push(entry); if pending.swap(true, AcqRel) == false { wake() }
//   consumer:  drain queue; pending.swap(false); re-check queue;
//              if non-empty { re-arm (swap(true)); loop }
//
// This is the classic arm/disarm idiom; the property that matters is no
// lost wakeup on the safety side: a non-empty queue is always either armed
// (a consumer cycle is owed) or the producer still owes its arm — and any
// producer arm that transitions the flag false->true issues the wake. The
// consumer's disarm-then-recheck closes the race with a push that lands
// while it is finishing: either the push's arm sees `armed == true` (the
// consumer's re-check will observe the queue) or it sees `false` and wakes.
//
// Transitions are one successful atomic step each; `arm_owed` models the
// producer's outstanding obligation between its push and its arm swap.

use vstd::prelude::*;
verus! {

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct WakeState {
    pub armed: bool,
    pub qlen: nat,
    /// a push landed whose arm swap has not happened yet
    pub arm_owed: bool,
    /// 0 = idle, 1 = draining, 2 = disarmed-and-rechecking
    pub consumer: u8,
}

pub open spec fn inv(s: WakeState) -> bool {
    s.qlen > 0nat ==> (s.armed || s.arm_owed || s.consumer == 2u8)
}

pub open spec fn step(s: WakeState, s2: WakeState) -> bool {
    // producer push (queue insert); the arm obligation is now owed
    (s2 == (WakeState { armed: s.armed, qlen: (s.qlen + 1nat) as nat, arm_owed: true, consumer: s.consumer })
        // producer arm swap returned true: flag was already up, no wake needed
        || (s.armed
            && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: false, consumer: s.consumer }))
        // producer arm swap returned false: flag transitions up, wake issued
        || (!s.armed
            && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: false, consumer: s.consumer }))
    // consumer drains one entry
    || (s.qlen > 0nat
        && s.consumer != 0u8
        && s2 == (WakeState { armed: s.armed, qlen: (s.qlen - 1nat) as nat, arm_owed: s.arm_owed, consumer: s.consumer }))
    // consumer disarms and enters the re-check window
    || (s.consumer == 1u8
        && s2 == (WakeState { armed: false, qlen: s.qlen, arm_owed: s.arm_owed, consumer: 2u8 }))
    // re-check saw a non-empty queue: re-arm (swap(true)) and loop
    || (s.consumer == 2u8
        && s.qlen > 0nat
        && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: s.arm_owed, consumer: 1u8 }))
    // re-check saw an empty queue: the consumer goes idle
    || (s.consumer == 2u8
        && s.qlen == 0nat
        && s2 == (WakeState { armed: false, qlen: 0nat, arm_owed: s.arm_owed, consumer: 0u8 })))
}

proof fn any_step_preserves_inv(s: WakeState, s2: WakeState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if s2 == (WakeState { armed: s.armed, qlen: (s.qlen + 1nat) as nat, arm_owed: true, consumer: s.consumer }) {
        // push: s2.qlen > 0 and arm_owed is set
        assert(s2.qlen > 0nat ==> s2.armed || s2.arm_owed || s2.consumer == 2u8);
    } else if s.armed
        && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: false, consumer: s.consumer }) {
        // arm-was-true: armed stays up
        assert(s2.qlen > 0nat ==> s2.armed);
    } else if !s.armed
        && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: false, consumer: s.consumer }) {
        // arm-was-false: wake issued, armed up
        assert(s2.qlen > 0nat ==> s2.armed);
    } else if s.qlen > 0nat
        && s.consumer != 0u8
        && s2 == (WakeState { armed: s.armed, qlen: (s.qlen - 1nat) as nat, arm_owed: s.arm_owed, consumer: s.consumer }) {
        // drain: either the queue empties or the disjunct flags survive
        if s2.qlen > 0nat {
            assert(s.armed || s.arm_owed || s.consumer == 2u8);
        }
    } else if s.consumer == 1u8
        && s2 == (WakeState { armed: false, qlen: s.qlen, arm_owed: s.arm_owed, consumer: 2u8 }) {
        // disarm: the re-check window covers a non-empty queue
        if s2.qlen > 0nat {
            assert(s2.consumer == 2u8);
        }
    } else if s.consumer == 2u8
        && s.qlen > 0nat
        && s2 == (WakeState { armed: true, qlen: s.qlen, arm_owed: s.arm_owed, consumer: 1u8 }) {
        // re-check non-empty: re-armed, loop continues
        assert(s2.qlen > 0nat ==> s2.armed);
    } else {
        // re-check empty: vacuous
        assert(s2.qlen == 0nat);
    }
}

/// THE NO-LOST-WAKEUP CORE: whenever the queue holds an entry, a consumer
/// cycle is owed — the flag is armed, the producer still owes its arm (and
/// its arm-swap wakes on a false->true transition), or the consumer is in
/// its re-check window where it re-arms and loops. No push can strand.
proof fn nonempty_queue_is_covered(s: WakeState)
    requires
        inv(s),
        s.qlen > 0nat,
    ensures
        s.armed || s.arm_owed || s.consumer == 2u8,
{
    assert(s.armed || s.arm_owed || s.consumer == 2u8);
}

/// When the consumer is idle, any queued entry is covered: either the flag
/// is armed (the consumer cycle is owed) or a producer owes its arm swap —
/// and a producer's swap on a false flag *issues the wake*, so the entry
/// cannot strand. The arm_owed case's resolution is a liveness step (the
/// producer eventually runs); the safety content is that no push can be
/// dropped by the flag protocol itself.
proof fn idle_consumer_entries_are_covered(s: WakeState)
    requires
        inv(s),
        s.consumer == 0u8,
    ensures
        s.qlen == 0nat || s.armed || s.arm_owed,
{
    if s.qlen > 0nat {
        assert(s.armed || s.arm_owed || s.consumer == 2u8);
        assert(s.consumer != 2u8);
        assert(s.armed || s.arm_owed);
    }
}

} // verus!
