//! Persistent worker pool for vector work: scoped parallel-for (graph
//! builds, exact scans) and fire-and-forget jobs (off-worker queries,
//! background rebuilds).
//!
//! Spawning OS threads per query or per build chunk cost tens of µs each;
//! these threads live for the process. High-priority jobs (queries,
//! parallel-for tickets) always run before low-priority ones (index
//! builds, compaction).

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct Queues {
    /// Chunks of an in-progress `parallel` call: pure compute that never
    /// waits on anything, so any thread may run one while it waits.
    tickets: VecDeque<Job>,
    high: VecDeque<Job>,
    low: VecDeque<Job>,
}

pub struct Pool {
    queues: Mutex<Queues>,
    cv: Condvar,
    threads: usize,
    busy: AtomicUsize,
    /// Jobs queued (both priorities); lets spinning workers poll without
    /// taking the lock.
    queued: AtomicUsize,
    /// Workers parked on `cv` (changed under the queue lock).
    sleepers: AtomicUsize,
}

/// How long an idle worker polls for new work before parking. A parked
/// worker costs a futex wake (several µs) per job; spinning this long
/// after each job keeps back-to-back queries off that path.
const SPIN: std::time::Duration = std::time::Duration::from_micros(30);

static CONFIG: OnceLock<(usize, Option<Vec<usize>>)> = OnceLock::new();
static GLOBAL: OnceLock<Arc<Pool>> = OnceLock::new();

/// Set the pool size and optional CPU list before first use (server
/// startup). Later calls are ignored.
pub fn configure(threads: usize, cores: Option<Vec<usize>>) {
    let _ = CONFIG.set((threads.max(1), cores.filter(|c| !c.is_empty())));
}

pub fn global() -> &'static Pool {
    GLOBAL.get_or_init(|| {
        let (threads, cores) = CONFIG
            .get()
            .cloned()
            .unwrap_or_else(|| (num_cpus::get().max(1), None));
        Pool::start(threads, cores)
    })
}

fn pin_current(core: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = core;
}

impl Pool {
    fn start(threads: usize, cores: Option<Vec<usize>>) -> Arc<Pool> {
        let pool = Arc::new(Pool {
            queues: Mutex::new(Queues::default()),
            cv: Condvar::new(),
            threads,
            busy: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            sleepers: AtomicUsize::new(0),
        });
        for i in 0..threads {
            let p = Arc::clone(&pool);
            let core = cores.as_ref().map(|c| c[i % c.len()]);
            std::thread::Builder::new()
                .name(format!("vec-pool-{i}"))
                .spawn(move || {
                    if let Some(c) = core {
                        pin_current(c);
                    }
                    p.worker_loop()
                })
                .expect("spawn vector pool thread");
        }
        pool
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Workers currently executing a job; a cheap load signal for callers
    /// deciding whether to fan out.
    pub fn busy(&self) -> usize {
        self.busy.load(Ordering::Relaxed)
    }

    fn pop(&self) -> Option<Job> {
        if self.queued.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut q = self.queues.lock().unwrap();
        let job = q.tickets.pop_front().or_else(|| q.high.pop_front()).or_else(|| q.low.pop_front());
        if job.is_some() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        job
    }

    fn push(&self, job: Job, low: bool) {
        let sleepers = {
            let mut q = self.queues.lock().unwrap();
            if low {
                q.low.push_back(job);
            } else {
                q.high.push_back(job);
            }
            self.queued.fetch_add(1, Ordering::AcqRel);
            self.sleepers.load(Ordering::Relaxed)
        };
        if sleepers > 0 {
            self.cv.notify_one();
        }
    }

    /// Run one queued `parallel` chunk if any is waiting. For threads that
    /// are blocked on something else (e.g. a batched scan another thread
    /// is performing) and would otherwise idle while holding a pool slot.
    pub fn try_run_ticket(&self) -> bool {
        if self.queued.load(Ordering::Acquire) == 0 {
            return false;
        }
        let job = {
            let mut q = self.queues.lock().unwrap();
            let job = q.tickets.pop_front();
            if job.is_some() {
                self.queued.fetch_sub(1, Ordering::AcqRel);
            }
            job
        };
        match job {
            Some(job) => {
                self.run(job);
                true
            }
            None => false,
        }
    }

    fn run(&self, job: Job) {
        self.busy.fetch_add(1, Ordering::Relaxed);
        let _ = catch_unwind(AssertUnwindSafe(job));
        self.busy.fetch_sub(1, Ordering::Relaxed);
    }

    fn worker_loop(&self) {
        loop {
            if let Some(job) = self.pop() {
                self.run(job);
                continue;
            }
            let start = std::time::Instant::now();
            let mut i = 0u32;
            while self.queued.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
                i = i.wrapping_add(1);
                if i % 64 == 0 && start.elapsed() >= SPIN {
                    break;
                }
            }
            if self.queued.load(Ordering::Acquire) != 0 {
                continue;
            }
            let mut q = self.queues.lock().unwrap();
            while q.tickets.is_empty() && q.high.is_empty() && q.low.is_empty() {
                self.sleepers.fetch_add(1, Ordering::Relaxed);
                q = self.cv.wait(q).unwrap();
                self.sleepers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Queue a latency-sensitive job (a query).
    pub fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        self.push(Box::new(job), false);
    }

    /// Queue a background job (builds, compaction): runs only on otherwise
    /// idle workers.
    pub fn spawn_low(&self, job: impl FnOnce() + Send + 'static) {
        self.push(Box::new(job), true);
    }

    /// Run `f(0..tasks)` across up to `tasks` threads (the caller takes
    /// part) and return once every task finished. Panics in any task are
    /// re-raised here.
    ///
    /// Workers pick up "tickets" that claim task indices from this call's
    /// own counter; the caller claims indices too and never runs unrelated
    /// queued jobs, so waiting here cannot re-enter other work (a queued
    /// query that itself waits on the caller would deadlock). Nesting is
    /// safe for the same reason: a caller can always finish its tasks alone.
    pub fn parallel(&self, tasks: usize, f: impl Fn(usize) + Sync) {
        if tasks == 0 {
            return;
        }
        if tasks == 1 {
            return f(0);
        }
        let shared = Arc::new(Shared {
            next: AtomicUsize::new(0),
            done: AtomicUsize::new(0),
            panicked: AtomicBool::new(false),
        });
        let f_ref: &(dyn Fn(usize) + Sync) = &f;
        // SAFETY: `f_static` is only called after claiming an index below
        // `tasks`, and this function does not return until all `tasks`
        // claimed indices have finished; a ticket that starts later claims
        // an out-of-range index and never touches `f_static`.
        let f_static: &'static (dyn Fn(usize) + Sync) = unsafe { std::mem::transmute(f_ref) };
        {
            let mut q = self.queues.lock().unwrap();
            for _ in 1..tasks {
                let shared = Arc::clone(&shared);
                q.tickets.push_back(Box::new(move || shared.drain(tasks, f_static)));
            }
            self.queued.fetch_add(tasks - 1, Ordering::AcqRel);
        }
        self.cv.notify_all();
        shared.drain(tasks, f_static);
        let mut spins = 0u32;
        while shared.done.load(Ordering::Acquire) < tasks {
            if spins < 128 {
                spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        if shared.panicked.load(Ordering::Relaxed) {
            panic!("vector pool task panicked");
        }
    }
}

struct Shared {
    next: AtomicUsize,
    done: AtomicUsize,
    panicked: AtomicBool,
}

impl Shared {
    fn drain(&self, tasks: usize, f: &(dyn Fn(usize) + Sync)) {
        loop {
            let i = self.next.fetch_add(1, Ordering::AcqRel);
            if i >= tasks {
                return;
            }
            if catch_unwind(AssertUnwindSafe(|| f(i))).is_err() {
                self.panicked.store(true, Ordering::Relaxed);
            }
            self.done.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_runs_every_task_once() {
        let pool = Pool::start(4, None);
        let hits: Vec<AtomicUsize> = (0..64).map(|_| AtomicUsize::new(0)).collect();
        pool.parallel(64, |i| {
            hits[i].fetch_add(1, Ordering::Relaxed);
        });
        assert!(hits.iter().all(|h| h.load(Ordering::Relaxed) == 1));
    }

    #[test]
    fn nested_parallel_does_not_deadlock() {
        let pool = Pool::start(2, None);
        let total = AtomicUsize::new(0);
        pool.parallel(8, |_| {
            pool.parallel(8, |_| {
                total.fetch_add(1, Ordering::Relaxed);
            });
        });
        assert_eq!(total.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn spawned_jobs_run() {
        let pool = Pool::start(2, None);
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..16 {
            let tx = tx.clone();
            if i % 2 == 0 {
                pool.spawn(move || tx.send(i).unwrap());
            } else {
                pool.spawn_low(move || tx.send(i).unwrap());
            }
        }
        let mut got: Vec<i32> = (0..16).map(|_| rx.recv().unwrap()).collect();
        got.sort();
        assert_eq!(got, (0..16).collect::<Vec<_>>());
    }
}
