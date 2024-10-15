use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
use parking_lot::lock_api::RawRwLock;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
#[cfg(feature = "process-stats")]
use {
    crate::memory_data_size::MemoryDataSize, nightly_quirks::utils::NightlyUtils,
    parking_lot::Mutex, std::cmp::max,
};

pub struct PhaseResult {
    name: String,
    time: Duration,
}

pub struct PhaseTimesMonitor {
    timer: Option<Instant>,
    phase: Option<(String, Instant)>,
    results: Vec<PhaseResult>,
}

pub static PHASES_TIMES_MONITOR: RwLock<PhaseTimesMonitor> =
    RwLock::const_new(parking_lot::RawRwLock::INIT, PhaseTimesMonitor::new());

#[cfg(feature = "process-stats")]
struct ProcessStats {
    user_cpu_total: f64,
    kernel_cpu_total: f64,
    mem_total: u128,
    mem_max: u64,
    samples_cnt: u64,
}

#[cfg(feature = "process-stats")]
impl ProcessStats {
    const fn new() -> Self {
        ProcessStats {
            user_cpu_total: 0.0,
            kernel_cpu_total: 0.0,
            mem_total: 0,
            mem_max: 0,
            samples_cnt: 0,
        }
    }

    fn update(
        &mut self,
        elapsed_time: Duration,
        elapsed_cpu: Duration,
        elapsed_kernel: Duration,
        current_mem: u64,
    ) {
        self.samples_cnt += 1;
        self.user_cpu_total += elapsed_cpu.as_secs_f64() / elapsed_time.as_secs_f64();
        self.kernel_cpu_total += elapsed_kernel.as_secs_f64() / elapsed_time.as_secs_f64();
        self.mem_total += current_mem as u128;
        self.mem_max = max(self.mem_max, current_mem);
    }

    fn format(&self) -> String {
        let samples_cnt = if self.samples_cnt == 0 {
            1
        } else {
            self.samples_cnt
        };

        format!(
            "(uc:{:.2} kc:{:.2} mm:{:.2} cm:{:.2})",
            (self.user_cpu_total / (samples_cnt as f64)),
            (self.kernel_cpu_total / (samples_cnt as f64)),
            MemoryDataSize::from_bytes(self.mem_max as usize),
            MemoryDataSize::from_bytes((self.mem_total / (samples_cnt as u128)) as usize)
        )
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[cfg(feature = "process-stats")]
static GLOBAL_STATS: Mutex<ProcessStats> = NightlyUtils::new_mutex(ProcessStats::new());
#[cfg(feature = "process-stats")]
static PHASE_STATS: Mutex<ProcessStats> = NightlyUtils::new_mutex(ProcessStats::new());
#[cfg(feature = "process-stats")]
static CURRENT_STATS: Mutex<ProcessStats> = NightlyUtils::new_mutex(ProcessStats::new());

impl PhaseTimesMonitor {
    const fn new() -> PhaseTimesMonitor {
        PhaseTimesMonitor {
            timer: None,
            phase: None,
            results: Vec::new(),
        }
    }

    pub fn init(&mut self) {
        self.timer = Some(Instant::now());

        #[cfg(feature = "process-stats")]
        {
            std::thread::spawn(|| {
                let clock = Instant::now();

                let mut last_stats = crate::simple_process_stats::ProcessStats::get().unwrap();
                let mut last_clock = clock.elapsed();

                loop {
                    std::thread::sleep(Duration::from_millis(100));
                    let stats = crate::simple_process_stats::ProcessStats::get().unwrap();

                    let time_now = clock.elapsed();

                    let elapsed = time_now - last_clock;
                    let kernel_elapsed_usage = stats.cpu_time_kernel - last_stats.cpu_time_kernel;
                    let user_elapsed_usage = stats.cpu_time_user - last_stats.cpu_time_user;
                    let current_memory = stats.memory_usage_bytes;

                    GLOBAL_STATS.lock().update(
                        elapsed,
                        user_elapsed_usage,
                        kernel_elapsed_usage,
                        current_memory,
                    );
                    PHASE_STATS.lock().update(
                        elapsed,
                        user_elapsed_usage,
                        kernel_elapsed_usage,
                        current_memory,
                    );

                    let mut current_stats = CURRENT_STATS.lock();
                    current_stats.reset();
                    current_stats.update(
                        elapsed,
                        user_elapsed_usage,
                        kernel_elapsed_usage,
                        current_memory,
                    );

                    last_clock = time_now;
                    last_stats = stats;
                }
            });
        }
    }

    fn end_phase(&mut self) {
        if let Some((name, phase_timer)) = self.phase.take() {
            let elapsed = phase_timer.elapsed();
            crate::log_info!(
                "Finished {}. phase duration: {:.2?} gtime: {:.2?}{}", // memory: {:.2} {:.2}%
                name,
                &elapsed,
                self.get_wallclock(),
                Self::format_process_stats()
            );
            self.results.push(PhaseResult {
                name,
                time: elapsed,
            })
        }
    }

    pub fn start_phase(&mut self, name: String) {
        self.end_phase();
        crate::log_info!(
            "Started {}{}{}",
            name,
            match () {
                #[cfg(feature = "process-stats")]
                () => "prev stats: ",
                #[cfg(not(feature = "process-stats"))]
                () => String::new(),
            },
            Self::format_process_stats()
        );
        #[cfg(feature = "process-stats")]
        PHASE_STATS.lock().reset();
        self.phase = Some((name, Instant::now()));
    }

    pub fn get_wallclock(&self) -> Duration {
        self.timer
            .as_ref()
            .map(|t| t.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }

    pub fn get_phase_desc(&self) -> String {
        self.phase
            .as_ref()
            .map(|x| x.0.clone())
            .unwrap_or(String::new())
    }

    pub fn get_phase_timer(&self) -> Duration {
        self.phase
            .as_ref()
            .map(|x| x.1.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }

    fn format_process_stats() -> String {
        #[cfg(feature = "process-stats")]
        {
            let memory = crate::simple_process_stats::ProcessStats::get()
                .unwrap()
                .memory_usage_bytes;

            format!(
                " GL:{} PH:{} CT: {} CM: {:.2}",
                GLOBAL_STATS.lock().format(),
                PHASE_STATS.lock().format(),
                CURRENT_STATS.lock().format(),
                MemoryDataSize::from_bytes(memory as usize),
            )
        }
        #[cfg(not(feature = "process-stats"))]
        String::new()
    }

    pub fn get_formatted_counter(&self) -> String {
        let total_mem = CHUNKS_ALLOCATOR.get_total_memory();
        let free_mem = CHUNKS_ALLOCATOR.get_free_memory();

        format!(
            " ptime: {:.2?} gtime: {:.2?} memory: {:.2} {:.2}%{}",
            self.phase
                .as_ref()
                .map(|pt| pt.1.elapsed())
                .unwrap_or(Duration::from_millis(0)),
            self.get_wallclock(),
            total_mem - free_mem,
            ((1.0 - free_mem / total_mem) * 100.0),
            Self::format_process_stats()
        )
    }

    pub fn get_formatted_counter_without_memory(&self) -> String {
        format!(
            " ptime: {:.2?} gtime: {:.2?}{}",
            self.phase
                .as_ref()
                .map(|pt| pt.1.elapsed())
                .unwrap_or(Duration::from_millis(0)),
            self.get_wallclock(),
            Self::format_process_stats()
        )
    }

    pub fn print_stats(&mut self, end_message: String) {
        self.end_phase();

        crate::log_info!("{}", end_message);
        crate::log_info!("TOTAL TIME: {:.2?}", self.get_wallclock());
        crate::log_info!("Final stats:");

        for PhaseResult { name, time } in self.results.iter() {
            crate::log_info!("\t{} \t=> {:.2?}", name, time);
        }
    }
}
