use std::sync::atomic::Ordering;

#[inline(always)]
pub(super) fn arm(pending: &super::AtomicBool) -> bool {
   pending.swap(true, Ordering::AcqRel)
}

#[inline(always)]
pub(super) fn disarm(pending: &super::AtomicBool) {
   // Exchange before checking the queue so both sides cannot miss the wake.
   pending.swap(false, Ordering::AcqRel);
}
