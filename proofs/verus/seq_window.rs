// Verified model of the AOF sequence-window allocator in
// src/storage/persistence/aof.rs (`Writer.seq_base/seq_next` +

use vstd::prelude::*;
verus! {

/// Window width (64 in the real AOF; 2 keeps the SMT domain small —
/// the proofs are size-generic).
pub open spec fn window_width() -> nat { 2nat }

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Win {
    pub primed: bool,
    /// first sequence of the current window (multiple of W)
    pub base: nat,
    /// next sequence to hand out; == end when the window is exhausted
    pub next: nat,
    /// exclusive end of the window: base + W
    pub end: nat,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct SeqState {
    pub global: nat,
    pub w0: Win,
    pub w1: Win,
}

pub open spec fn win_inv(w: Win) -> bool {
    (!w.primed ==> (w.base == 0nat && w.next == 0nat && w.end == 0nat))
        && (w.primed ==> (w.base <= w.next && w.next <= w.end && w.end == w.base + window_width()))
}

pub open spec fn inv(s: SeqState) -> bool {
    win_inv(s.w0)
        && win_inv(s.w1)
        && s.global % window_width() == 0nat
        // every primed window was carved out of the global counter
        && (s.w0.primed ==> s.w0.end <= s.global)
        && (s.w1.primed ==> s.w1.end <= s.global)
        // distinct refill moments give distinct, W-disjoint bases
        && ((s.w0.primed && s.w1.primed) ==> (
            s.w0.base == s.w1.base
            || s.w0.base + window_width() <= s.w1.base
            || s.w1.base + window_width() <= s.w0.base))
}

pub open spec fn refill_w0(s: SeqState, s2: SeqState) -> bool {
    s.w0.next == s.w0.end
        && s2 == (SeqState {
            global: s.global + window_width(),
            w0: Win {
                primed: true,
                base: s.global,
                next: s.global,
                end: s.global + window_width(),
            },
            w1: s.w1,
        })
}

pub open spec fn alloc_w0(s: SeqState, s2: SeqState) -> bool {
    s.w0.primed
        && s.w0.next < s.w0.end
        && s2 == (SeqState {
            global: s.global,
            w0: Win {
                primed: true,
                base: s.w0.base,
                next: s.w0.next + 1nat,
                end: s.w0.end,
            },
            w1: s.w1,
        })
}

pub open spec fn step(s: SeqState, s2: SeqState) -> bool {
    refill_w0(s, s2)
        || alloc_w0(s, s2)
        || (s.w1.next == s.w1.end
            && s2 == (SeqState {
                global: s.global + window_width(),
                w0: s.w0,
                w1: Win {
                    primed: true,
                    base: s.global,
                    next: s.global,
                    end: s.global + window_width(),
                },
            }))
        || (s.w1.primed
            && s.w1.next < s.w1.end
            && s2 == (SeqState {
                global: s.global,
                w0: s.w0,
                w1: Win {
                    primed: true,
                    base: s.w1.base,
                    next: s.w1.next + 1nat,
                    end: s.w1.end,
                },
            }))
}

/// A handed-out sequence always sits inside its writer's window.
pub open spec fn in_window(w: Win, seq: nat) -> bool {
    w.primed && w.base <= seq && seq < w.end
}

proof fn any_step_preserves_inv(s: SeqState, s2: SeqState)
    requires
        inv(s),
        step(s, s2),
    ensures
        inv(s2),
{
    if refill_w0(s, s2) {
        // the new window is [global, global + W): global advances by W so
        // it stays a multiple of W; the other writer's window (if primed)
        assert(s.w1.primed ==> s.w1.end <= s.global);
        assert(s2.global % window_width() == 0nat);
        assert(s2.w0.end <= s2.global);
        if s.w1.primed {
            assert(s2.w0.base == s.global);
            assert(s.w1.base + window_width() <= s.global);
            assert(s2.w1.base + window_width() <= s2.w0.base);
        }
    } else if alloc_w0(s, s2) {
        assert(s2.global % window_width() == 0nat);
        assert(s2.w0.next <= s2.w0.end);
        assert(s2.w0.end == s2.w0.base + window_width());
        if s.w1.primed {
            assert(s2.w0.base == s.w0.base);
            assert(s2.w1.base == s.w1.base);
            if s.w0.base == s.w1.base {
            } else if s.w0.base + window_width() <= s.w1.base {
            } else {
                assert(s.w1.base + window_width() <= s.w0.base);
            }
        }
    } else if s.w1.next == s.w1.end
        && s2 == (SeqState {
            global: s.global + window_width(),
            w0: s.w0,
            w1: Win {
                primed: true,
                base: s.global,
                next: s.global,
                end: s.global + window_width(),
            },
        }) {
        assert(s.w0.primed ==> s.w0.end <= s.global);
        assert(s2.global % window_width() == 0nat);
        assert(s2.w1.end <= s2.global);
        if s.w0.primed {
            assert(s2.w1.base == s.global);
            assert(s.w0.base + window_width() <= s.global);
            assert(s2.w0.base + window_width() <= s2.w1.base);
        }
    } else {
        // alloc_w1: no counter change
        assert(s2.global % window_width() == 0nat);
        assert(s2.w1.next <= s2.w1.end);
        assert(s2.w1.end == s2.w1.base + window_width());
    }
}

/// THE UNIQUENESS THEOREM for the replay merge: the two writers' current
/// windows never overlap, so a sequence handed out by one writer can never
proof fn windows_are_disjoint(s: SeqState)
    requires
        inv(s),
        s.w0.primed,
        s.w1.primed,
        s.w0.base != s.w1.base,
    ensures
        forall |a: nat, b: nat|
            (in_window(s.w0, a) && in_window(s.w1, b)) ==> a != b,
{
    // windows are half-open ranges of width W whose bases are distinct
    // multiples of W, so the ranges are disjoint
    assert(s.w0.base + window_width() <= s.w1.base || s.w1.base + window_width() <= s.w0.base);
    assert(forall |a: nat, b: nat|
        (in_window(s.w0, a) && in_window(s.w1, b)) ==> a != b);
}

/// Every handed-out sequence sits below the global counter, which only
/// advances in W-wide refills — the ranges tile the sequence space without
proof fn handed_out_seqs_are_below_global(s: SeqState, s2: SeqState, a: nat)
    requires
        inv(s),
        step(s, s2),
        alloc_w0(s, s2),
        a == s.w0.next,
    ensures
        in_window(s.w0, a) && a < s2.global,
{
    assert(in_window(s.w0, a));
    assert(a < s.w0.end);
    assert(s.w0.end <= s.global);
    assert(s2.global == s.global);
}

} // verus!
