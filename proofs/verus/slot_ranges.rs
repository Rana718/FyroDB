// Models remove_slot and the adjacent-range branch of compact_ranges in src/cluster/state.rs.
use vstd::prelude::*;

verus! {
    #[derive(Copy, Clone)]
    struct Range { lo: u16, hi: u16 }

    spec fn valid(r: Range) -> bool {
        r.lo <= r.hi && r.hi < 16384
    }

    spec fn contains(r: Range, k: u16) -> bool {
        r.lo <= k && k <= r.hi
    }

    spec fn in_part(part: Option<Range>, k: u16) -> bool {
        match part { Some(r) => contains(r, k), None => false }
    }

    fn split(r: Range, slot: u16) -> (parts: (Option<Range>, Option<Range>))
        requires valid(r),
        ensures
            forall |k: u16| (in_part(parts.0, k) || in_part(parts.1, k))
                == (contains(r, k) && k != slot),
            forall |k: u16| !(in_part(parts.0, k) && in_part(parts.1, k)),
            parts.0.is_some() ==> valid(parts.0.unwrap()),
            parts.1.is_some() ==> valid(parts.1.unwrap()),
    {
        if slot < r.lo || slot > r.hi {
            return (Some(r), None);
        }
        let left = if r.lo < slot { Some(Range { lo: r.lo, hi: slot - 1 }) } else { None };
        let right = if slot < r.hi { Some(Range { lo: slot + 1, hi: r.hi }) } else { None };
        (left, right)
    }

    fn merge_if_adjacent(a: Range, b: Range) -> (merged: Option<Range>)
        requires valid(a), valid(b),
        ensures
            merged.is_some() == (a.hi as int + 1 == b.lo as int),
            merged.is_some() ==> valid(merged.unwrap()),
            merged.is_some() ==> forall |k: u16| contains(merged.unwrap(), k)
                == (contains(a, k) || contains(b, k)),
    {
        if a.hi + 1 == b.lo { Some(Range { lo: a.lo, hi: b.hi }) } else { None }
    }
}
