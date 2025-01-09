// use std::sync::atomic::AtomicUsize;

// use parking_lot::{Condvar, Mutex};

// type BlockingTask = (Mutex<usize>, Condvar);
// type AsyncTask = (tokio::sync::Mutex<usize>, tokio::sync::Notify);

// pub struct WorkToken {
//     priority: usize,
//     blocking: bool,
// }

// pub struct PriorityScheduler;

// pub const MAX_PRIORITY: usize = 4;

// static PRIORITIES: once_cell::sync::Lazy<Vec<(BlockingTask, AsyncTask)>> =
//     once_cell::sync::Lazy::new(|| {
//         (0..MAX_PRIORITY)
//             .map(|_| {
//                 (
//                     (Mutex::new(0), Condvar::new()),
//                     (tokio::sync::Mutex::new(0), tokio::sync::Notify::new()),
//                 )
//             })
//             .collect()
//     });

// static MAX_THREADS_COUNT: AtomicUsize = AtomicUsize::new(usize::MAX);
// static RUNNING_THREADS_COUNT_GUARD: Mutex<usize> = Mutex::new(0);
// static RUNNING_THREADS_COUNT_CONDVAR: Condvar = Condvar::new();

// impl PriorityScheduler {
//     pub fn set_max_threads_count(threads_count: usize) {
//         MAX_THREADS_COUNT.store(threads_count, std::sync::atomic::Ordering::Relaxed);
//     }

//     // Checks if there is an higher priority task to execute and in case wakes it and returns false, else returns true
//     fn wake_higher_priorities(priority: usize, async_workaround: bool) -> bool {
//         let mut can_execute = true;
//         // Check if there is a task with an higher priority
//         for p in (priority + 1..MAX_PRIORITY).rev() {
//             // Blocking case
//             let (mutex, condvar) = &PRIORITIES[p].0;

//             let blocking_waiting_count = mutex.lock();
//             if *blocking_waiting_count > 0 {
//                 condvar.notify_one();
//                 can_execute = false;
//                 break;
//             }
//             drop(blocking_waiting_count);

//             // Async case
//             let (mutex, notify) = &PRIORITIES[p].1;
//             let async_waiting_count = if async_workaround {
//                 loop {
//                     if let Ok(guard) = mutex.try_lock() {
//                         break guard;
//                     }
//                     std::thread::yield_now();
//                 }
//             } else {
//                 mutex.blocking_lock()
//             };
//             if *async_waiting_count > 0 {
//                 notify.notify_one();
//                 can_execute = false;
//                 break;
//             }
//         }
//         can_execute
//     }

//     async fn wake_higher_priorities_async(priority: usize) -> bool {
//         let mut can_execute = true;
//         // Check if there is a task with an higher priority
//         for p in (priority + 1..MAX_PRIORITY).rev() {
//             // Blocking case
//             let (mutex, condvar) = &PRIORITIES[p].0;

//             let blocking_waiting_count = mutex.lock();
//             if *blocking_waiting_count > 0 {
//                 condvar.notify_one();
//                 can_execute = false;
//                 break;
//             }
//             drop(blocking_waiting_count);

//             // Async case
//             let (mutex, notify) = &PRIORITIES[p].1;
//             let async_waiting_count = mutex.lock().await;
//             if *async_waiting_count > 0 {
//                 notify.notify_one();
//                 can_execute = false;
//                 break;
//             }
//         }
//         can_execute
//     }

//     fn wait_for_thread_count() {
//         let mut running_threads_count = RUNNING_THREADS_COUNT_GUARD.lock();
//         while *running_threads_count >= MAX_THREADS_COUNT.load(std::sync::atomic::Ordering::Relaxed)
//         {
//             RUNNING_THREADS_COUNT_CONDVAR.wait(&mut running_threads_count);
//         }
//         *running_threads_count += 1;
//     }

//     pub fn declare_usage_blocking(priority: usize) -> WorkToken {
//         let mut own_mutex_guard = PRIORITIES[priority].0 .0.lock();
//         *own_mutex_guard += 1;

//         loop {
//             let can_execute = Self::wake_higher_priorities(priority, false);
//             if can_execute {
//                 break;
//             }
//             PRIORITIES[priority].0 .1.wait(&mut own_mutex_guard);
//         }

//         *own_mutex_guard -= 1;
//         drop(own_mutex_guard);
//         Self::wait_for_thread_count();

//         WorkToken {
//             priority,
//             blocking: true,
//         }
//     }

//     pub async fn declare_usage_async(priority: usize) -> WorkToken {
//         let mut own_mutex_guard = PRIORITIES[priority].1 .0.lock().await;
//         *own_mutex_guard += 1;

//         loop {
//             let can_execute = Self::wake_higher_priorities_async(priority).await;
//             if can_execute {
//                 break;
//             }
//             // Taken from https://github.com/kaimast/tokio-condvar
//             let notified_future = PRIORITIES[priority].1 .1.notified();
//             tokio::pin!(notified_future);
//             notified_future.as_mut().enable();
//             drop(own_mutex_guard);
//             notified_future.await;
//             own_mutex_guard = PRIORITIES[priority].1 .0.lock().await;
//         }

//         *own_mutex_guard -= 1;
//         Self::wait_for_thread_count();

//         WorkToken {
//             priority,
//             blocking: false,
//         }
//     }
// }

// impl Drop for WorkToken {
//     fn drop(&mut self) {
//         let mut running_threads_count = RUNNING_THREADS_COUNT_GUARD.lock();
//         *running_threads_count -= 1;
//         RUNNING_THREADS_COUNT_CONDVAR.notify_one();
//         drop(running_threads_count);
//         PriorityScheduler::wake_higher_priorities(self.priority, !self.blocking);
//     }
// }
