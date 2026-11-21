// Model one successful atomic sequence allocation, up to u64 exhaustion.
use vstd::prelude::*;
verus! {
    struct Allocation {
        before: nat,
        issued: nat,
        after: nat,
    }

    spec fn allocate(a: Allocation) -> bool {
        a.before < u64::MAX && a.issued == a.before && a.after == a.before + 1
    }

    proof fn ordered_allocations_are_distinct(first: Allocation, second: Allocation)
        requires allocate(first), allocate(second), first.after <= second.before,
        ensures first.issued < second.issued,
    {}
}
