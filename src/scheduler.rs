use std::{future::Future, sync::atomic::AtomicUsize};

// use parking_lot::{Condvar, Mutex};

pub struct ThreadPriorityHandle {
    #[allow(dead_code)]
    priority: usize,
}
pub struct PriorityScheduler;

pub const MAX_PRIORITY: usize = 4;

static MAX_THREADS_COUNT: AtomicUsize = AtomicUsize::new(usize::MAX);
// static RUNNING_THREADS_COUNT_GUARD: Mutex<usize> = Mutex::new(0);
// static RUNNING_THREADS_COUNT_CONDVAR: Condvar = Condvar::new();

impl PriorityScheduler {
    /// Declares a new thread that does busy work with an execution priority (lower => more priority)
    pub fn declare_thread(priority: usize) -> ThreadPriorityHandle {
        // Register the thread as running
        // *RUNNING_THREADS_COUNT_GUARD.lock() += 1;
        ThreadPriorityHandle { priority }
    }

    /// Sets the maximum allowed number of running threads
    pub fn set_max_threads_count(threads_count: usize) {
        MAX_THREADS_COUNT.store(threads_count, std::sync::atomic::Ordering::Relaxed);
    }

    fn decrease_running_threads_count() {
        // let mut running_threads_count = RUNNING_THREADS_COUNT_GUARD.lock();
        // *running_threads_count -= 1;

        // if *running_threads_count < MAX_THREADS_COUNT.load(Ordering::Relaxed) {
        //     // Wake a sleeping thread
        //     RUNNING_THREADS_COUNT_CONDVAR.notify_one();
        // }
    }

    fn wait_waking() {
        // let mut running_threads_count = RUNNING_THREADS_COUNT_GUARD.lock();
        // while *running_threads_count >= MAX_THREADS_COUNT.load(Ordering::Relaxed) {
        //     // Wait for a thread to finish
        //     RUNNING_THREADS_COUNT_CONDVAR.wait(&mut running_threads_count);
        // }
        // *running_threads_count += 1;
    }

    /// Declares that the current thread is waiting inside the lambda, not counting it in the running threads
    pub fn execute_blocking_call<T>(
        _handle: &ThreadPriorityHandle,
        waiting_fn: impl FnOnce() -> T,
    ) -> T {
        // Decrease the running threads count
        Self::decrease_running_threads_count();
        let result = waiting_fn();
        // Increase the running threads count
        // *RUNNING_THREADS_COUNT_GUARD.lock() += 1;
        result
    }

    /// Declares that the current thread is waiting inside the lambda, not counting it in the running threads
    pub async fn execute_blocking_call_async<T>(
        _handle: &ThreadPriorityHandle,
        waiting_fn: impl Future<Output = T>,
    ) -> T {
        // Decrease the running threads count
        Self::decrease_running_threads_count();
        let result = waiting_fn.await;
        // Wait for threads waking
        Self::wait_waking();
        result
    }
}

impl Drop for ThreadPriorityHandle {
    fn drop(&mut self) {
        // The thread is terminating, decrease the running threads count
        PriorityScheduler::decrease_running_threads_count();
    }
}
