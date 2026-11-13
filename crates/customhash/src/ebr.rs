use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicUsize, Ordering};

static PENDING: AtomicUsize = AtomicUsize::new(0);
thread_local! {
   static DIRTY: Cell<bool> = const { Cell::new(false) };
}

pub struct Guard {
   inner: ManuallyDrop<crossbeam_epoch::Guard>,
}

impl Drop for Guard {
   #[inline]
   fn drop(&mut self) {
      // Release this guard exactly once before testing whether its thread quiesced.
      unsafe { ManuallyDrop::drop(&mut self.inner) };
      if DIRTY.try_with(Cell::get).unwrap_or(false) && !crossbeam_epoch::is_pinned() {
         force_collect();
      }
   }
}

#[inline(always)]
pub fn pin() -> Guard {
   Guard {
      inner: ManuallyDrop::new(crossbeam_epoch::pin()),
   }
}

struct Completed;
impl Drop for Completed {
   fn drop(&mut self) {
      PENDING.fetch_sub(1, Ordering::Release);
   }
}

/// # Safety
/// The callback must free the unlinked allocation once and be safe to run on any thread.
#[inline]
pub unsafe fn retire_raw(ptr: *mut u8, drop_fn: unsafe fn(*mut u8)) {
   if ptr.is_null() {
      return;
   }
   let guard = pin();
   PENDING.fetch_add(1, Ordering::Relaxed);
   unsafe {
      guard.inner.defer_unchecked(move || {
         let _completed = Completed;
         drop_fn(ptr);
      });
   }
   let _ = DIRTY.try_with(|dirty| dirty.set(true));
}

pub fn force_collect() {
   let pinned = crossbeam_epoch::is_pinned();
   if !pinned {
      let _ = DIRTY.try_with(|dirty| dirty.set(false));
   }
   let mut previous = PENDING.load(Ordering::Acquire);
   let mut stalled = 0;
   for _ in 0..16_384 {
      let guard = crossbeam_epoch::pin();
      guard.flush();
      drop(guard);
      let remaining = PENDING.load(Ordering::Acquire);
      if remaining == 0 {
         if !pinned {
            let _ = DIRTY.try_with(|dirty| dirty.set(false));
         }
         return;
      }
      if remaining >= previous {
         stalled += 1;
         if stalled >= 4 {
            return;
         }
      } else {
         stalled = 0;
      }
      previous = remaining;
      std::thread::yield_now();
   }
}

pub fn force_collect_quiescent() {
   for _ in 0..64 {
      force_collect();
      if PENDING.load(Ordering::Acquire) == 0 {
         return;
      }
      std::thread::yield_now();
   }
}

#[inline(always)]
pub fn with_pin<R>(f: impl FnOnce(&()) -> R) -> R {
   let _guard = pin();
   f(&())
}

#[cfg(test)]
pub(crate) static TEST_SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
   use super::TEST_SERIALIZE;
   use std::sync::atomic::{AtomicUsize, Ordering};

   static DROPPED: AtomicUsize = AtomicUsize::new(0);
   static CHAIN_DROPPED: AtomicUsize = AtomicUsize::new(0);

   unsafe fn count_drop(ptr: *mut u8) {
      drop(unsafe { Box::from_raw(ptr.cast::<u64>()) });
      DROPPED.fetch_add(1, Ordering::Release);
   }

   fn retire_on_new_thread(count: usize) {
      std::thread::spawn(move || {
         for value in 0..count as u64 {
            let leaked = Box::into_raw(Box::new(value)).cast::<u8>();
            unsafe { super::retire_raw(leaked, count_drop) };
         }
      })
      .join()
      .unwrap();
   }

   fn wait_for(counter: &AtomicUsize, target: usize) {
      for _ in 0..400 {
         super::force_collect();
         if counter.load(Ordering::Acquire) >= target {
            break;
         }
         std::thread::sleep(std::time::Duration::from_millis(5));
      }
      assert_eq!(counter.load(Ordering::Acquire), target);
   }

   #[test]
   fn garbage_from_exited_threads_is_still_reclaimed() {
      let _serial = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
      let before = DROPPED.load(Ordering::Acquire);
      for _ in 0..2 {
         retire_on_new_thread(16);
      }
      wait_for(&DROPPED, before + 32);
   }

   #[test]
   fn deferred_values_are_reclaimed_across_thread_churn() {
      let _serial = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
      let before = DROPPED.load(Ordering::Acquire);
      for _ in 0..64 {
         retire_on_new_thread(4);
      }
      wait_for(&DROPPED, before + 256);
   }

   unsafe fn reentrant_drop(ptr: *mut u8) {
      let value = *unsafe { Box::from_raw(ptr.cast::<u64>()) };
      CHAIN_DROPPED.fetch_add(1, Ordering::Release);
      let _guard = super::pin();
      if value != 0 {
         let next = Box::into_raw(Box::new(value - 1)).cast();
         unsafe { super::retire_raw(next, reentrant_drop) };
      }
   }

   #[test]
   fn reclamation_callbacks_can_pin_and_retire_more_objects() {
      let _serial = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
      let before = CHAIN_DROPPED.load(Ordering::Acquire);
      let ptr = Box::into_raw(Box::new(31u64)).cast();
      unsafe { super::retire_raw(ptr, reentrant_drop) };
      wait_for(&CHAIN_DROPPED, before + 32);
   }
}
#[cfg(test)]
mod panic_regression {
   use std::sync::atomic::{AtomicUsize, Ordering};
   static DROPPED: AtomicUsize = AtomicUsize::new(0);
   unsafe fn drop_value(ptr: *mut u8) {
      drop(unsafe { Box::from_raw(ptr.cast::<u64>()) });
      DROPPED.fetch_add(1, Ordering::Release);
   }

   #[test]
   fn a_panicking_callback_releases_its_pin_before_later_collection() {
      let _serial = super::TEST_SERIALIZE
         .lock()
         .unwrap_or_else(|e| e.into_inner());
      let before = DROPPED.load(Ordering::Acquire);
      std::thread::spawn(move || {
         let result = std::panic::catch_unwind(|| super::with_pin(|_| panic!("callback failed")));
         assert!(result.is_err());
         let ptr = Box::into_raw(Box::new(7u64)).cast();
         unsafe { super::retire_raw(ptr, drop_value) };
         for _ in 0..200 {
            super::force_collect();
            if DROPPED.load(Ordering::Acquire) == before + 1 {
               return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
         }
         assert_eq!(DROPPED.load(Ordering::Acquire), before + 1);
      })
      .join()
      .unwrap();
   }
}
