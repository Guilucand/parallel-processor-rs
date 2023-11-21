use std::collections::BTreeMap;

use crate::memory_fs::file::flush::*;
use std::path::Path;

use self::file::fs_interface::{DefaultFsInterface, FsInterface};
use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
use crate::memory_fs::file::fs_interface::FileReaderInterface;
use crate::memory_fs::file::internal::{MemoryFileInternal, SWAPPABLE_FILES};
use nightly_quirks::utils::NightlyUtils;

#[macro_use]
pub mod allocator;
pub mod file;
pub mod flushable_buffer;

pub struct MemoryFs;

#[derive(Copy, Clone)]
pub enum RemoveFileMode {
    Keep,
    Remove { remove_fs: bool },
}

pub(crate) static FILE_SYSTEM: DefaultFsInterface = DefaultFsInterface::new();

impl MemoryFs {
    pub fn init(
        base_path: impl AsRef<Path>,
        memory_size: MemoryDataSize,
        flush_queue_size: usize,
        threads_count: usize,
        min_chunks_count: usize,
    ) {
        let chunk_size = (memory_size / (min_chunks_count as f64)).as_bytes();

        let mut suggested_chunk_size_log = 1;

        while (1 << (suggested_chunk_size_log + 1)) <= chunk_size {
            suggested_chunk_size_log += 1;
        }

        FILE_SYSTEM.set_base_dir(base_path.as_ref());

        CHUNKS_ALLOCATOR.initialize(memory_size, suggested_chunk_size_log, min_chunks_count);
        GlobalFlush::init(flush_queue_size, threads_count);
    }

    pub fn remove_file(name: &str, remove_mode: RemoveFileMode) -> Result<(), ()> {
        match remove_mode {
            RemoveFileMode::Keep => {
                // Do nothing
                Ok(())
            }
            RemoveFileMode::Remove { remove_fs } => {
                if MemoryFileInternal::delete(name, remove_fs) {
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    }

    pub fn get_file_size(name: &str) -> Option<usize> {
        MemoryFileInternal::retrieve_reference(name)
            .map(|f| f.read().len())
            .or_else(|| FILE_SYSTEM.open_file(name).map(|f| f.len()).ok())
    }

    pub fn flush_all_to_disk() {
        GlobalFlush::flush_to_disk();
    }

    pub fn free_memory() {
        CHUNKS_ALLOCATOR.giveback_free_memory()
    }

    pub fn terminate() {
        GlobalFlush::terminate();
        CHUNKS_ALLOCATOR.deinitialize();
    }

    pub fn reduce_pressure() -> bool {
        // println!("Reducing pressure!");
        let (current, max_size) = GlobalFlush::global_queue_occupation();
        if current * 3 < max_size {
            let mut map_lock = SWAPPABLE_FILES.lock();
            let map_lock_mut = NightlyUtils::mutex_get_or_init(&mut map_lock, || BTreeMap::new());

            let mut file_ref = None;

            for (key, file) in map_lock_mut.iter() {
                if let Some(file) = file.upgrade() {
                    let file_read = file.read();
                    if file_read.is_memory_preferred() && file_read.has_flush_pending_chunks() {
                        drop(file_read);
                        file_ref = Some((key.clone(), file));
                        break;
                    }
                }
            }

            if let Some((key, file)) = file_ref {
                map_lock_mut.remove(&key);
                drop(map_lock);
                let mut file = file.write();
                file.change_to_disk_only();
                file.flush_chunks(usize::MAX);
                return true;
            }
        }

        return !GlobalFlush::is_queue_empty();
    }
}

#[cfg(test)]
mod tests {
    use crate::memory_data_size::MemoryDataSize;
    use crate::memory_fs::file::flush::GlobalFlush;
    use crate::memory_fs::file::internal::MemoryFileMode;
    use crate::memory_fs::file::reader::FileReader;
    use crate::memory_fs::file::writer::FileWriter;
    use crate::memory_fs::MemoryFs;
    use rayon::prelude::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    #[ignore]
    pub fn memory_fs_test() {
        MemoryFs::init(
            "/tmp/",
            MemoryDataSize::from_mebioctets(100 * 1024),
            1024,
            3,
            0,
        );
        let data = (0..3337).map(|x| (x % 256) as u8).collect::<Vec<u8>>();

        (0..400).into_par_iter().for_each(|i: u32| {
            println!("Writing file {}", i);
            let mut file = FileWriter::create(
                format!("/home/andrea/genome-assembly/test1234/{}.tmp", i),
                MemoryFileMode::PreferMemory { swap_priority: 3 },
            );
            for _ in 0..(1024 * 64) {
                file.write(data.as_slice()).unwrap();
            }
            drop(file);
            let mut file2 = FileReader::open(
                format!("/home/andrea/genome-assembly/test1234/{}.tmp", i),
                None,
            )
            .unwrap();

            file2.seek(SeekFrom::Start(17 + 3337 * 12374)).unwrap();
            let mut buffer = [0; 4];
            file2.read_exact(&mut buffer).unwrap();
            assert_eq!(&buffer, &data[17..21]);
        });

        GlobalFlush::flush_to_disk();

        (0..400).into_par_iter().for_each(|i: u32| {
            println!("Reading file {}", i);
            let mut datar = vec![0; 3337];
            let mut file = FileReader::open(
                format!("/home/andrea/genome-assembly/test1234/{}.tmp", i),
                None,
            )
            .unwrap();
            for _ in 0..(1024 * 64) {
                file.read_exact(datar.as_mut_slice()).unwrap();
                assert_eq!(datar, data);
            }
            assert_eq!(file.read(datar.as_mut_slice()).unwrap(), 0);
            println!("Read file {}", i);
        });

        MemoryFs::terminate();
    }
}
