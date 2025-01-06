use crate::buckets::bucket_writer::BucketItemSerializer;
use crate::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use crate::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use crate::memory_fs::RemoveFileMode;
use crossbeam::channel::*;
use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard};
use std::cmp::min;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use super::BucketReader;

#[derive(Clone)]
enum OpenedFile {
    NotOpened,
    Plain(Arc<LockFreeBinaryReader>),
    Compressed(Arc<CompressedBinaryReader>),
    Finished,
}

impl OpenedFile {
    pub fn is_finished(&self) -> bool {
        match self {
            OpenedFile::NotOpened => false,
            OpenedFile::Finished => true,
            OpenedFile::Plain(f) => f.is_finished(),
            OpenedFile::Compressed(f) => f.is_finished(),
        }
    }

    #[allow(dead_code)]
    pub fn get_path(&self) -> PathBuf {
        match self {
            OpenedFile::Plain(f) => f.get_name(),
            OpenedFile::Compressed(f) => f.get_name(),
            _ => panic!("File not opened"),
        }
    }

    pub fn get_chunks_count(&self) -> usize {
        match self {
            OpenedFile::Plain(file) => file.get_chunks_count(),
            OpenedFile::Compressed(file) => file.get_chunks_count(),
            OpenedFile::NotOpened | OpenedFile::Finished => 0,
        }
    }
}

pub struct AsyncReaderThread {
    buffers: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    buffers_pool: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    opened_file: Mutex<OpenedFile>,
    file_wait_condvar: Condvar,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl AsyncReaderThread {
    pub fn new(buffers_size: usize, buffers_count: usize) -> Arc<Self> {
        let buffers_pool = bounded(buffers_count);

        for _ in 0..buffers_count {
            buffers_pool
                .0
                .send(Vec::with_capacity(buffers_size))
                .unwrap();
        }

        Arc::new(Self {
            buffers: bounded(buffers_count),
            buffers_pool,
            opened_file: Mutex::new(OpenedFile::Finished),
            file_wait_condvar: Condvar::new(),
            thread: Mutex::new(None),
        })
    }

    fn read_thread(self: Arc<Self>) {
        let mut current_stream_compr = None;
        let mut current_stream_uncompr = None;

        while Arc::strong_count(&self) > 1 {
            let mut file = self.opened_file.lock();
            let mut buffer = self.buffers_pool.1.recv().unwrap();
            unsafe {
                buffer.set_len(buffer.capacity());
            }

            let bytes_read = match &mut *file {
                OpenedFile::NotOpened | OpenedFile::Finished => {
                    self.file_wait_condvar
                        .wait_for(&mut file, Duration::from_secs(5));
                    let _ = self.buffers_pool.0.send(buffer);
                    continue;
                }
                OpenedFile::Plain(file) => {
                    let mut last_read = usize::MAX;
                    let mut total_read_bytes = 0;
                    while total_read_bytes < buffer.len() {
                        if current_stream_uncompr.is_none() || last_read == 0 {
                            current_stream_uncompr = file.get_read_parallel_stream();
                            if current_stream_uncompr.is_none() {
                                break;
                            }
                        }
                        last_read = current_stream_uncompr
                            .as_mut()
                            .unwrap()
                            .read(&mut buffer[total_read_bytes..])
                            .unwrap();
                        total_read_bytes += last_read;
                    }
                    total_read_bytes
                }
                OpenedFile::Compressed(file) => {
                    let mut last_read = usize::MAX;
                    let mut total_read_bytes = 0;
                    while total_read_bytes < buffer.len() {
                        if current_stream_compr.is_none() || last_read == 0 {
                            current_stream_compr = file.get_read_parallel_stream();
                            if current_stream_compr.is_none() {
                                break;
                            }
                        }
                        last_read = current_stream_compr
                            .as_mut()
                            .unwrap()
                            .read(&mut buffer[total_read_bytes..])
                            .unwrap();
                        total_read_bytes += last_read;
                    }
                    total_read_bytes
                }
            };

            unsafe {
                buffer.set_len(bytes_read);
            }

            // File completely read
            if bytes_read == 0 {
                *file = OpenedFile::Finished;
            }

            let _ = self.buffers.0.send(buffer);
        }
    }

    fn read_bucket(self: Arc<Self>, new_opened_file: OpenedFile) -> AsyncStreamThreadReader {
        let mut opened_file = self.opened_file.lock();

        // Ensure that the previous file is finished
        match &*opened_file {
            OpenedFile::Finished => {}
            _ => panic!("File not finished!"),
        }

        *opened_file = new_opened_file;

        self.file_wait_condvar.notify_all();
        drop(opened_file);

        let stream_recv = self.buffers.1.clone();
        let owner = self.clone();

        let mut thread = self.thread.lock();
        let mt_self = self.clone();
        if thread.is_none() {
            *thread = Some(
                std::thread::Builder::new()
                    .name(String::from("async_reader"))
                    .spawn(move || {
                        mt_self.read_thread();
                    })
                    .unwrap(),
            );
        }
        drop(thread);

        let current = stream_recv.recv().unwrap();

        AsyncStreamThreadReader {
            receiver: stream_recv,
            owner,
            current,
            current_pos: 0,
        }
    }
}

pub struct AsyncStreamThreadReader {
    receiver: Receiver<Vec<u8>>,
    owner: Arc<AsyncReaderThread>,
    current: Vec<u8>,
    current_pos: usize,
}

impl Read for AsyncStreamThreadReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            if self.current_pos == self.current.len() {
                if self.current.len() == 0 {
                    return Ok(bytes_read);
                }
                let next = self.receiver.recv().unwrap();
                let _ = self
                    .owner
                    .buffers_pool
                    .0
                    .send(std::mem::replace(&mut self.current, next));
                self.current_pos = 0;
                continue;
            }

            let avail = self.current.len() - self.current_pos;
            let to_read = min(buf.len() - bytes_read, avail);
            buf[bytes_read..(bytes_read + to_read)]
                .copy_from_slice(&mut self.current[self.current_pos..(self.current_pos + to_read)]);
            bytes_read += to_read;
            self.current_pos += to_read;

            if bytes_read == buf.len() {
                return Ok(bytes_read);
            }
        }
    }
}

impl Drop for AsyncStreamThreadReader {
    fn drop(&mut self) {
        let _ = self
            .owner
            .buffers_pool
            .0
            .send(std::mem::take(&mut self.current));
    }
}

pub struct AsyncBinaryReader {
    path: PathBuf,
    opened_file: RwLock<OpenedFile>,
    compressed: bool,
    remove_file: RemoveFileMode,
    prefetch: Option<usize>,
}

impl AsyncBinaryReader {
    fn open_file(
        path: &PathBuf,
        compressed: bool,
        remove_file: RemoveFileMode,
        prefetch: Option<usize>,
    ) -> OpenedFile {
        if compressed {
            OpenedFile::Compressed(Arc::new(CompressedBinaryReader::new(
                path,
                remove_file,
                None,
            )))
        } else {
            OpenedFile::Plain(Arc::new(LockFreeBinaryReader::new(
                path,
                remove_file,
                prefetch,
            )))
        }
    }

    pub fn new(
        path: &PathBuf,
        compressed: bool,
        remove_file: RemoveFileMode,
        prefetch: Option<usize>,
    ) -> Self {
        Self {
            path: path.clone(),
            opened_file: RwLock::new(OpenedFile::NotOpened),
            compressed,
            remove_file,
            prefetch,
        }
    }

    pub fn get_chunks_count(&self) -> usize {
        let tmp_file;
        let opened_file = &self.opened_file.read();
        let file = match opened_file.deref() {
            OpenedFile::NotOpened | OpenedFile::Finished => {
                tmp_file = Self::open_file(&self.path, self.compressed, RemoveFileMode::Keep, None);
                &tmp_file
            }
            file => file,
        };

        file.get_chunks_count()
    }

    pub fn get_file_size(&self) -> usize {
        let tmp_file;
        let opened_file = &self.opened_file.read();
        let file = match opened_file.deref() {
            OpenedFile::NotOpened | OpenedFile::Finished => {
                tmp_file = Self::open_file(&self.path, self.compressed, RemoveFileMode::Keep, None);
                &tmp_file
            }
            file => file,
        };

        match file {
            OpenedFile::Plain(file) => file.get_length(),
            OpenedFile::Compressed(file) => file.get_length(),
            OpenedFile::NotOpened | OpenedFile::Finished => 0,
        }
    }
}

impl AsyncBinaryReader {
    pub fn is_finished(&self) -> bool {
        self.opened_file.read().is_finished()
    }

    pub fn get_items_stream<S: BucketItemSerializer>(
        &self,
        read_thread: Arc<AsyncReaderThread>,
        buffer: S::ReadBuffer,
        extra_buffer: S::ExtraDataBuffer,
    ) -> AsyncBinaryReaderItemsIterator<S> {
        let mut opened_file = self.opened_file.read();
        if matches!(*self.opened_file.read(), OpenedFile::NotOpened) {
            drop(opened_file);
            let mut writable = self.opened_file.write();
            if matches!(*writable, OpenedFile::NotOpened) {
                *writable =
                    Self::open_file(&self.path, self.compressed, self.remove_file, self.prefetch);
            }
            opened_file = RwLockWriteGuard::downgrade(writable);
        }

        let stream = read_thread.read_bucket(opened_file.clone());
        AsyncBinaryReaderItemsIterator {
            buffer,
            extra_buffer,
            stream,
            deserializer: S::new(),
        }
    }

    pub fn get_name(&self) -> PathBuf {
        self.path.clone()
    }
}

pub struct AsyncBinaryReaderItemsIterator<S: BucketItemSerializer> {
    buffer: S::ReadBuffer,
    extra_buffer: S::ExtraDataBuffer,
    stream: AsyncStreamThreadReader,
    deserializer: S,
}

impl<S: BucketItemSerializer> AsyncBinaryReaderItemsIterator<S> {
    pub fn next(&mut self) -> Option<(S::ReadType<'_>, &mut S::ExtraDataBuffer)> {
        let item = self.deserializer.read_from(
            &mut self.stream,
            &mut self.buffer,
            &mut self.extra_buffer,
        )?;
        Some((item, &mut self.extra_buffer))
    }
}
