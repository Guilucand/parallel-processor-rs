use super::{Error, ProcessStats};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

/// The fields of `/proc/self/stat` needed by the stats
#[derive(Debug, PartialEq, Eq)]
struct StatFields {
    utime: u64,
    stime: u64,
    rss: u64,
}

/// Parses a `/proc/[pid]/stat` line. Its second field, the process name, is enclosed in
/// parentheses and can itself contain spaces and parentheses, so the fields are counted from the
/// last `)`: the first one after it is field 3, the process state.
fn parse_stat(contents: &[u8]) -> Option<StatFields> {
    let name_end = contents.iter().rposition(|&byte| byte == b')')?;
    let after_name = std::str::from_utf8(&contents[name_end + 1..]).ok()?;
    let fields: Vec<&str> = after_name.split_ascii_whitespace().collect();
    let field = |number: usize| fields.get(number - 3)?.parse::<u64>().ok();

    Some(StatFields {
        utime: field(14)?,
        stime: field(15)?,
        rss: field(24)?,
    })
}

/// A value the kernel passes to every process in its auxiliary vector, or the `sysconf` value if
/// it is missing there
fn auxiliary_value(key: libc::c_ulong, sysconf_name: libc::c_int) -> u64 {
    match unsafe { libc::getauxval(key) } {
        0 => unsafe { libc::sysconf(sysconf_name) }.max(0) as u64,
        value => value as u64,
    }
}

fn page_size() -> u64 {
    auxiliary_value(libc::AT_PAGESZ, libc::_SC_PAGESIZE)
}

fn ticks_per_second() -> u64 {
    auxiliary_value(libc::AT_CLKTCK, libc::_SC_CLK_TCK)
}

pub fn get_info() -> Result<ProcessStats, Error> {
    let bytes_per_page = page_size();
    let ticks_per_second = ticks_per_second();

    let path = Path::new("/proc/self/stat");
    let mut file_contents = Vec::new();
    File::open(path)
        .map_err(|e| Error::FileRead(path.to_path_buf(), e))?
        .read_to_end(&mut file_contents)
        .map_err(|e| Error::FileRead(path.to_path_buf(), e))?;

    let stat = parse_stat(&file_contents).ok_or(Error::FileContentsMalformed)?;

    let memory_usage_bytes = stat.rss * bytes_per_page;
    let user_mode_seconds = (stat.utime as f64) / (ticks_per_second as f64);
    let kernel_mode_seconds = (stat.stime as f64) / (ticks_per_second as f64);

    Ok(ProcessStats {
        cpu_time_user: Duration::from_secs_f64(user_mode_seconds),
        cpu_time_kernel: Duration::from_secs_f64(kernel_mode_seconds),
        memory_usage_bytes,
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use procfs::{process::Stat, FromRead};

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

    fn procfs_fields(contents: &[u8]) -> Option<StatFields> {
        let stat = Stat::from_read(contents).ok()?;
        Some(StatFields {
            utime: stat.utime,
            stime: stat.stime,
            rss: stat.rss,
        })
    }

    /// The same fields, page size and clock ticks as procfs
    #[test]
    pub fn matches_procfs() {
        let contents = std::fs::read("/proc/self/stat").unwrap();
        assert!(parse_stat(&contents).is_some());
        assert_eq!(parse_stat(&contents), procfs_fields(&contents));
        assert_eq!(page_size(), procfs::page_size());
        assert_eq!(ticks_per_second(), procfs::ticks_per_second());
    }

    /// Spaces and parentheses in the process name do not shift the fields
    #[test]
    pub fn process_names_with_parentheses() {
        let contents = std::fs::read_to_string("/proc/self/stat").unwrap();
        let name_start = contents.find('(').unwrap();
        let name_end = contents.rfind(')').unwrap();

        for name in ["", "a b", "a) b", "(a) (b)", ")) ((", "1 2 3 4 5 6 7 8 9 10 11 12 13"] {
            let line = format!(
                "{}({}){}",
                &contents[..name_start],
                name,
                &contents[name_end + 1..]
            );
            assert!(parse_stat(line.as_bytes()).is_some(), "name {:?}", name);
            assert_eq!(
                parse_stat(line.as_bytes()),
                procfs_fields(line.as_bytes()),
                "name {:?}",
                name
            );
        }
    }

    /// A name that is not valid UTF-8 is skipped, as only the fields after it are decoded
    #[test]
    pub fn process_name_not_utf8() {
        let contents = std::fs::read("/proc/self/stat").unwrap();
        let name_start = contents.iter().position(|&byte| byte == b'(').unwrap();
        let name_end = contents.iter().rposition(|&byte| byte == b')').unwrap();

        let mut line = contents[..=name_start].to_vec();
        line.extend_from_slice(&[0xff, 0xfe, b' ', 0x80]);
        line.extend_from_slice(&contents[name_end..]);
        assert_eq!(parse_stat(&line), parse_stat(&contents));
    }

    #[test]
    pub fn malformed_lines() {
        assert_eq!(parse_stat(b""), None);
        assert_eq!(parse_stat(b"1 (name) S 0 0"), None);
        // Fields 3 to 23, without rss (field 24)
        let contents = std::fs::read_to_string("/proc/self/stat").unwrap();
        let name_end = contents.rfind(')').unwrap();
        let fields: Vec<&str> = contents[name_end + 1..]
            .split_ascii_whitespace()
            .take(21)
            .collect();
        let without_rss = format!("{} {}", &contents[..=name_end], fields.join(" "));
        assert_eq!(parse_stat(without_rss.as_bytes()), None);
    }
}
