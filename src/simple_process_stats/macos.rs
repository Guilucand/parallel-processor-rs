use super::{Error, ProcessStats};
use std::time::Duration;

/// `mach_timebase_info_data_t`, declared here as the libc crate deprecates its own declaration
#[repr(C)]
struct MachTimebaseInfo {
    numer: u32,
    denom: u32,
}

extern "C" {
    fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
}

pub fn get_info() -> Result<ProcessStats, Error> {
    let pid = unsafe { libc::getpid() };
    let mut task_info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            &mut task_info as *mut libc::proc_taskinfo as *mut libc::c_void,
            size,
        )
    };
    if written != size {
        return Err(Error::SystemCall(std::io::Error::last_os_error()));
    }

    // The task times are in Mach absolute time units: nanoseconds on Intel, longer on Apple Silicon
    let mut timebase = MachTimebaseInfo { numer: 0, denom: 0 };
    if unsafe { mach_timebase_info(&mut timebase) } != 0 || timebase.denom == 0 {
        return Err(Error::SystemCall(std::io::Error::last_os_error()));
    }
    let to_duration = |ticks: u64| {
        Duration::from_nanos(
            (ticks as u128 * timebase.numer as u128 / timebase.denom as u128) as u64,
        )
    };

    Ok(ProcessStats {
        memory_usage_bytes: task_info.pti_resident_size,
        cpu_time_user: to_duration(task_info.pti_total_user),
        cpu_time_kernel: to_duration(task_info.pti_total_system),
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    pub fn test_no_error() {
        #[no_mangle]
        fn spin_for_a_bit() {
            let mut _a = 0;
            for _i in 0..9999999 {
                _a += 1;
            }
        }

        // to get some nonzero number for cpu_time_user
        spin_for_a_bit();

        dbg!(get_info().unwrap());
    }
}
