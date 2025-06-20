use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::file::flush::GlobalFlush;
use crate::memory_fs::flushable_buffer::{FileFlushMode, FlushableItem};
use crate::memory_fs::stats;
use dashmap::DashMap;
use filebuffer::FileBuffer;
use once_cell::sync::Lazy;
use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{Mutex, RawRwLock, RwLock};
use replace_with::replace_with_or_abort;
use rustc_hash::FxHashMap;
use std::cmp::min;
use std::collections::BinaryHeap;
use std::fs::remove_file;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};

use super::handle::FileHandle;

static MEMORY_MAPPED_FILES: Lazy<DashMap<PathBuf, Arc<RwLock<MemoryFileInternal>>>> =
    Lazy::new(|| DashMap::new());

#[derive(Default)]
pub struct SwappableFiles {
    index: FxHashMap<PathBuf, Weak<RwLock<MemoryFileInternal>>>,
    queue: BinaryHeap<(usize, PathBuf)>,
}

impl SwappableFiles {
    pub fn add_file(
        &mut self,
        priority: usize,
        path: PathBuf,
        file: Weak<RwLock<MemoryFileInternal>>,
    ) {
        self.index.insert(path.clone(), file);
        self.queue.push((priority, path));
    }

    pub fn remove_file(&mut self, path: &Path) {
        self.index.remove(path);
    }

    pub fn get_next(&mut self) -> Option<Arc<RwLock<MemoryFileInternal>>> {
        while let Some((_, file)) = self.queue.pop() {
            if let Some(file_entry) = self.index.remove(&file) {
                if let Some(file_entry) = file_entry.upgrade() {
                    let file_read = file_entry.read();
                    if file_read.is_memory_preferred() && file_read.has_flush_pending_chunks() {
                        drop(file_read);
                        return Some(file_entry);
                    } else {
                        // Remove the file if it cannot be swapped
                        file_read.on_swap_list.store(false, Ordering::Relaxed);
                    }
                }
            }
        }

        None
    }
}

pub static SWAPPABLE_FILES: LazyLock<Mutex<SwappableFiles>> =
    LazyLock::new(|| Mutex::new(Default::default()));

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum MemoryFileMode {
    AlwaysMemory,
    PreferMemory { swap_priority: usize },
    DiskOnly,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum OpenMode {
    None,
    Read,
    Write,
}

// struct RwLockIterator<'a, A, B, I: Iterator<Item = B>> {
//     _lock: RwLockReadGuard<'a, A>,
//     iterator: I,
// }

// impl<'a, A, B, I: Iterator<Item = B>> Iterator for RwLockIterator<'a, A, B, I> {
//     type Item = B;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iterator.next()
//     }
// }

// pub struct MemoryFileChunksIterator<'a, I: Iterator<Item = &'a mut [u8]>> {
//     iter: I,
// }

// impl<'a, I: Iterator<Item = &'a mut [u8]>> Iterator for MemoryFileChunksIterator<'a, I> {
//     type Item = &'a mut [u8];

//     #[inline(always)]
//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next()
//     }
// }

pub enum FileChunk {
    OnDisk { offset: u64, len: usize },
    OnMemory { chunk: AllocatedChunk },
}

impl FileChunk {
    pub fn get_length(&self) -> usize {
        match self {
            FileChunk::OnDisk { len, .. } => *len,
            FileChunk::OnMemory { chunk } => chunk.len(),
        }
    }

    #[inline(always)]
    pub fn get_ptr(&self, file: &UnderlyingFile, prefetch: Option<usize>) -> *const u8 {
        unsafe {
            match self {
                FileChunk::OnDisk { offset, .. } => {
                    if let UnderlyingFile::ReadMode(file) = file {
                        let file = file.as_ref().unwrap();

                        if let Some(prefetch) = prefetch {
                            let remaining_length = file.len() - *offset as usize;
                            let prefetch_length = min(remaining_length, prefetch);
                            file.prefetch(*offset as usize, prefetch_length);
                        }

                        file.as_ptr().add(*offset as usize)
                    } else {
                        panic!("Error, wrong underlying file!");
                    }
                }
                FileChunk::OnMemory { chunk } => chunk.get_mut_ptr() as *const u8,
            }
        }
    }
}

pub enum UnderlyingFile {
    NotOpened,
    MemoryOnly,
    MemoryPreferred,
    WriteMode {
        file: Arc<Mutex<FileHandle>>,
        chunk_position: usize,
    },
    ReadMode(Option<FileBuffer>),
}

pub struct MemoryFileInternal {
    /// Path associated with the current file
    path: PathBuf,
    /// Disk read/write structure
    file: UnderlyingFile,
    /// Memory mode
    memory_mode: MemoryFileMode,
    /// Read or write mode
    open_mode: (OpenMode, usize),
    /// Actual memory mapping
    memory: Vec<Arc<RwLock<FileChunk>>>,
    /// True if it's in the swap list
    on_swap_list: AtomicBool,
    /// True if more chunks can be flushed
    can_flush: bool,
}

impl MemoryFileInternal {
    pub fn create_new(path: impl AsRef<Path>, memory_mode: MemoryFileMode) -> Arc<RwLock<Self>> {
        let new_file = Arc::new(RwLock::new(Self {
            path: path.as_ref().into(),
            file: UnderlyingFile::NotOpened,
            memory_mode,
            open_mode: (OpenMode::None, 0),
            memory: Vec::new(),
            on_swap_list: AtomicBool::new(false),
            can_flush: true,
        }));

        MEMORY_MAPPED_FILES.insert(path.as_ref().into(), new_file.clone());

        new_file
    }

    pub fn create_from_fs(path: impl AsRef<Path>) -> Option<Arc<RwLock<Self>>> {
        if !path.as_ref().exists() || !path.as_ref().is_file() {
            return None;
        }
        let len = path.as_ref().metadata().ok()?.len() as usize;

        let new_file = Arc::new(RwLock::new(Self {
            path: path.as_ref().into(),
            file: UnderlyingFile::NotOpened,
            memory_mode: MemoryFileMode::DiskOnly,
            open_mode: (OpenMode::None, 0),
            memory: vec![Arc::new(RwLock::new(FileChunk::OnDisk { offset: 0, len }))],
            on_swap_list: AtomicBool::new(false),
            can_flush: false,
        }));

        MEMORY_MAPPED_FILES.insert(path.as_ref().into(), new_file.clone());

        Some(new_file)
    }

    pub fn debug_dump_files() {
        for file in MEMORY_MAPPED_FILES.iter() {
            let file = file.read();
            crate::log_info!(
                "File '{}' => chunks: {}",
                file.path.display(),
                file.memory.len()
            );
        }
    }

    pub fn is_on_disk(&self) -> bool {
        self.memory_mode == MemoryFileMode::DiskOnly
    }

    pub fn is_memory_preferred(&self) -> bool {
        if let MemoryFileMode::PreferMemory { .. } = self.memory_mode {
            true
        } else {
            false
        }
    }

    pub fn get_chunk(&self, index: usize) -> Arc<RwLock<FileChunk>> {
        self.memory[index].clone()
    }

    pub fn get_chunks_count(&self) -> usize {
        self.memory.len()
    }

    pub fn retrieve_reference(path: impl AsRef<Path>) -> Option<Arc<RwLock<Self>>> {
        MEMORY_MAPPED_FILES.get(path.as_ref()).map(|f| f.clone())
    }

    pub fn active_files_count() -> usize {
        MEMORY_MAPPED_FILES.len()
    }

    pub fn delete(path: impl AsRef<Path>, remove_fs: bool) -> bool {
        if let Some(file) = MEMORY_MAPPED_FILES.remove(path.as_ref()) {
            stats::decrease_files_usage(file.1.read().len() as u64);
            if remove_fs {
                match file.1.read().memory_mode {
                    MemoryFileMode::AlwaysMemory => {}
                    MemoryFileMode::PreferMemory { .. } => {
                        SWAPPABLE_FILES.lock().remove_file(path.as_ref())
                    }
                    MemoryFileMode::DiskOnly => {
                        if let Ok(file_meta) = std::fs::metadata(path.as_ref()) {
                            stats::decrease_disk_usage(file_meta.len());
                        }
                        let _ = remove_file(path);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn delete_directory(dir: impl AsRef<Path>, remove_fs: bool) -> bool {
        let mut all_succeeded = true;
        let mut to_delete = vec![];
        for file in MEMORY_MAPPED_FILES.iter() {
            if file.key().starts_with(&dir) {
                to_delete.push(file.key().clone());
            }
        }

        for file in to_delete {
            all_succeeded &= Self::delete(&file, remove_fs);
        }

        all_succeeded
    }

    fn create_writing_underlying_file(path: &Path) -> UnderlyingFile {
        // Remove the file if it existed from a previous run
        let _ = remove_file(path);

        UnderlyingFile::WriteMode {
            file: Arc::new(Mutex::new(FileHandle::new(path.to_path_buf()))),
            chunk_position: 0,
        }
    }

    pub fn open(&mut self, mode: OpenMode) -> Result<(), String> {
        if self.open_mode.0 == mode {
            self.open_mode.1 += 1;
            return Ok(());
        }

        if self.open_mode.0 != OpenMode::None {
            return Err(format!("File {} is already opened!", self.path.display()));
        }

        {
            let mut error = None;
            replace_with_or_abort(&mut self.file, |file| {
                match mode {
                    OpenMode::None => UnderlyingFile::NotOpened,
                    OpenMode::Read => {
                        self.open_mode = (OpenMode::Read, 1);
                        self.can_flush = false;

                        if self.memory_mode != MemoryFileMode::DiskOnly {
                            UnderlyingFile::MemoryOnly
                        } else {
                            // Ensure that all chunks are not pending
                            for chunk in self.memory.iter() {
                                drop(chunk.read());
                            }

                            if let UnderlyingFile::WriteMode { file, .. } = file {
                                file.lock().flush().unwrap();
                            }

                            UnderlyingFile::ReadMode(
                                FileBuffer::open(&self.path)
                                    .inspect_err(|e| {
                                        error = Some(format!(
                                            "Error while opening file {}: {}",
                                            self.path.display(),
                                            e
                                        ));
                                    })
                                    .ok(),
                            )
                        }
                    }
                    OpenMode::Write => {
                        self.open_mode = (OpenMode::Write, 1);
                        match self.memory_mode {
                            MemoryFileMode::AlwaysMemory => UnderlyingFile::MemoryOnly,
                            MemoryFileMode::PreferMemory { .. } => UnderlyingFile::MemoryPreferred,
                            MemoryFileMode::DiskOnly => {
                                Self::create_writing_underlying_file(&self.path)
                            }
                        }
                    }
                }
            });
            if let Some(error) = error {
                return Err(error);
            }
        }

        Ok(())
    }

    pub fn close(&mut self) {
        self.open_mode.1 -= 1;

        if self.open_mode.1 == 0 {
            self.open_mode.0 = OpenMode::None;
            match &self.file {
                UnderlyingFile::WriteMode { file, .. } => {
                    file.lock().flush().unwrap();
                }
                _ => {}
            }
        }
    }

    fn put_on_swappable_list(self_: &ArcRwLockWriteGuard<RawRwLock, Self>) {
        if let MemoryFileMode::PreferMemory { swap_priority } = self_.memory_mode {
            if !self_.on_swap_list.swap(true, Ordering::Relaxed) {
                SWAPPABLE_FILES.lock().add_file(
                    swap_priority,
                    self_.path.clone(),
                    Arc::downgrade(ArcRwLockWriteGuard::rwlock(self_)),
                );
            }
        }
    }

    pub fn reserve_space(
        self_: &Arc<RwLock<Self>>,
        last_chunk: AllocatedChunk,
        out_chunks: &mut Vec<(Option<ArcRwLockReadGuard<RawRwLock, FileChunk>>, &mut [u8])>,
        mut size: usize,
        el_size: usize,
    ) -> AllocatedChunk {
        let mut chunk = last_chunk;

        loop {
            let rem_bytes = chunk.remaining_bytes();
            let rem_elements = rem_bytes / el_size;
            let el_bytes = min(size, rem_elements * el_size);

            assert!(chunk.max_len() >= el_size);

            let space = if el_bytes > 0 {
                Some(unsafe { chunk.prealloc_bytes_single_thread(el_bytes) })
            } else {
                None
            };

            size -= el_bytes;

            let mut self_ = self_.write_arc();

            if size > 0 {
                self_
                    .memory
                    .push(Arc::new(RwLock::new(FileChunk::OnMemory { chunk })));

                if let Some(space) = space {
                    let chunk_guard = self_.memory.last().unwrap().read_arc();
                    out_chunks.push((Some(chunk_guard), space));
                }
                Self::put_on_swappable_list(&self_);

                drop(self_);
                chunk = CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(FileBuffer {
                    path: std::panic::Location::caller().to_string()
                }));
            } else {
                if let Some(space) = space {
                    out_chunks.push((None, space));
                }
                return chunk;
            }
        }
    }

    pub fn get_underlying_file(&self) -> &UnderlyingFile {
        &self.file
    }

    pub fn add_chunk(self_: &Arc<RwLock<Self>>, chunk: AllocatedChunk) {
        let mut self_ = self_.write_arc();
        self_
            .memory
            .push(Arc::new(RwLock::new(FileChunk::OnMemory { chunk })));
        Self::put_on_swappable_list(&self_);
    }

    pub fn flush_chunks(&mut self, limit: usize) -> usize {
        if !self.can_flush {
            return 0;
        }

        match &self.file {
            UnderlyingFile::NotOpened | UnderlyingFile::MemoryPreferred => {
                self.file = Self::create_writing_underlying_file(&self.path);
            }
            _ => {}
        }

        if let UnderlyingFile::WriteMode {
            file,
            chunk_position,
        } = &mut self.file
        {
            {
                let mut flushed_count = 0;
                while flushed_count < limit {
                    if *chunk_position >= self.memory.len() {
                        return flushed_count;
                    }

                    if let Some(flushable_chunk) = self.memory[*chunk_position].try_write_arc() {
                        GlobalFlush::add_item_to_flush_queue(FlushableItem {
                            underlying_file: file.clone(),
                            mode: FileFlushMode::Append {
                                chunk: flushable_chunk,
                            },
                        });
                        *chunk_position += 1;
                        flushed_count += 1;
                    } else {
                        return flushed_count;
                    }
                }
                return flushed_count;
            }
        }

        return 0;
    }

    pub fn flush_pending_chunks_count(&self) -> usize {
        match &self.file {
            UnderlyingFile::NotOpened
            | UnderlyingFile::MemoryOnly
            | UnderlyingFile::ReadMode(_) => 0,
            UnderlyingFile::WriteMode { chunk_position, .. } => {
                self.get_chunks_count() - *chunk_position
            }
            UnderlyingFile::MemoryPreferred => self.get_chunks_count(),
        }
    }

    #[inline(always)]
    pub fn has_flush_pending_chunks(&self) -> bool {
        self.flush_pending_chunks_count() > 0
    }

    pub fn change_to_disk_only(&mut self) {
        if self.is_memory_preferred() {
            self.memory_mode = MemoryFileMode::DiskOnly;
            self.file = Self::create_writing_underlying_file(&self.path);
        }
    }

    #[inline(always)]
    pub fn has_only_one_chunk(&self) -> bool {
        self.memory.len() == 1
    }

    #[inline(always)]
    pub fn get_path(&self) -> &Path {
        self.path.as_ref()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.memory
            .iter()
            .map(|x| match x.read().deref() {
                FileChunk::OnDisk { len, .. } => *len,
                FileChunk::OnMemory { chunk } => chunk.len(),
            })
            .sum::<usize>()
    }
}

unsafe impl Sync for MemoryFileInternal {}
unsafe impl Send for MemoryFileInternal {}
