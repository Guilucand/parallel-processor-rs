use crate::buckets::bucket_writer::BucketItemSerializer;
use crate::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use crate::buckets::readers::generic_binary_reader::{ChunkDecoder, GenericChunkedBinaryReader};
use crate::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use crate::buckets::CheckpointStrategy;
use crate::memory_fs::RemoveFileMode;
use crossbeam::channel::*;
use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard};
use serde::de::DeserializeOwned;
use std::cmp::min;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use super::generic_binary_reader::ChunkReader;
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

pub enum AsyncReaderBuffer {
    Passtrough {
        file_range: std::ops::Range<u64>,
        checkpoint_data: Option<Vec<u8>>,
    },
    Decompressed {
        data: Vec<u8>,
        checkpoint_data: Option<Vec<u8>>,
        is_continuation: bool,
    },
    Closed,
}

impl Default for AsyncReaderBuffer {
    fn default() -> Self {
        Self::Passtrough {
            file_range: 0..0,
            checkpoint_data: None,
        }
    }
}

impl AsyncReaderBuffer {
    fn into_buffer(self) -> Option<Vec<u8>> {
        match self {
            AsyncReaderBuffer::Passtrough { .. } | AsyncReaderBuffer::Closed => None,
            AsyncReaderBuffer::Decompressed { data, .. } => Some(data),
        }
    }
    fn is_continuation(&self) -> bool {
        match self {
            AsyncReaderBuffer::Passtrough { .. } | AsyncReaderBuffer::Closed => false,
            AsyncReaderBuffer::Decompressed {
                is_continuation, ..
            } => *is_continuation,
        }
    }
}

pub struct AsyncReaderThread {
    buffers: (Sender<AsyncReaderBuffer>, Receiver<AsyncReaderBuffer>),
    buffers_pool: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    opened_file: Mutex<(OpenedFile, CheckpointStrategy)>,
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
            opened_file: Mutex::new((OpenedFile::Finished, CheckpointStrategy::Decompress)),
            file_wait_condvar: Condvar::new(),
            thread: Mutex::new(None),
        })
    }

    fn read_thread(self: Arc<Self>) {
        let mut current_stream_compr = None;
        let mut current_stream_uncompr = None;

        while Arc::strong_count(&self) > 1 {
            let mut file_guard = self.opened_file.lock();

            let mut buffer = self.buffers_pool.1.recv().unwrap();
            unsafe {
                buffer.set_len(buffer.capacity());
            }
            let mut cached_buffer = Some(buffer);

            fn read_buffer<D: ChunkDecoder>(
                file: &GenericChunkedBinaryReader<D>,
                stream: &mut Option<ChunkReader<Vec<u8>, D::ReaderType>>,
                checkpoint_strategy: CheckpointStrategy,
                cached_buffer: &mut Option<Vec<u8>>,
            ) -> Option<AsyncReaderBuffer> {
                let mut last_read = usize::MAX;
                let mut total_read_bytes = 0;
                let mut checkpoint_data = None;
                let is_continuation = stream.is_some();

                let out_buffer = loop {
                    if stream.is_none() {
                        *stream = file.get_read_parallel_stream(checkpoint_strategy);

                        match &stream {
                            Some(stream_) => match stream_ {
                                ChunkReader::Reader(_, data) => checkpoint_data = data.clone(),
                                ChunkReader::Passtrough { file_range, data } => {
                                    // Just pass the file range and take the current stream
                                    let file_range = file_range.clone();
                                    let checkpoint_data = data.clone();

                                    stream.take();
                                    return Some(AsyncReaderBuffer::Passtrough {
                                        file_range,
                                        checkpoint_data,
                                    });
                                }
                            },
                            // File finished
                            None => return None,
                        }
                    }

                    let reader_stream = stream.as_mut().unwrap();

                    let out_buffer = cached_buffer.as_mut().unwrap();
                    while total_read_bytes < out_buffer.len() && last_read > 0 {
                        last_read = match reader_stream {
                            ChunkReader::Reader(reader, _) => {
                                reader.read(&mut out_buffer[total_read_bytes..]).unwrap()
                            }
                            _ => unreachable!(),
                        };
                        total_read_bytes += last_read;
                    }

                    if last_read == 0 {
                        // Current stream finished
                        stream.take();
                    }

                    // Avoid passing 0-sized buffers
                    if total_read_bytes > 0 {
                        out_buffer.truncate(total_read_bytes);
                        break cached_buffer.take().unwrap();
                    }
                };

                Some(AsyncReaderBuffer::Decompressed {
                    data: out_buffer,
                    checkpoint_data,
                    is_continuation,
                })
            }

            let checkpoint_strategy = file_guard.1;

            let data = match &mut file_guard.0 {
                OpenedFile::NotOpened | OpenedFile::Finished => {
                    self.file_wait_condvar
                        .wait_for(&mut file_guard, Duration::from_secs(5));
                    let _ = self.buffers_pool.0.send(cached_buffer.take().unwrap());
                    continue;
                }
                OpenedFile::Plain(file) => read_buffer(
                    &file,
                    &mut current_stream_uncompr,
                    checkpoint_strategy,
                    &mut cached_buffer,
                ),
                OpenedFile::Compressed(file) => read_buffer(
                    file,
                    &mut current_stream_compr,
                    checkpoint_strategy,
                    &mut cached_buffer,
                ),
            };

            match data {
                Some(data) => {
                    let _ = self.buffers.0.send(data);
                }
                None => {
                    // File completely read
                    current_stream_compr = None;
                    current_stream_uncompr = None;
                    file_guard.0 = OpenedFile::Finished;
                    let _ = self.buffers.0.send(AsyncReaderBuffer::Closed);
                }
            }

            if let Some(buffer) = cached_buffer {
                // Add back the buffer to the pool if it was not used
                let _ = self.buffers_pool.0.send(buffer);
            }
        }
    }

    fn read_bucket(
        self: Arc<Self>,
        new_opened_file: OpenedFile,
        checkpoint_strategy: CheckpointStrategy,
    ) -> AsyncStreamThreadReader {
        let mut opened_file = self.opened_file.lock();

        // Ensure that the previous file is finished
        match &opened_file.0 {
            OpenedFile::Finished => {}
            _ => panic!("File not finished!"),
        }

        *opened_file = (new_opened_file, checkpoint_strategy);

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
            checkpoint_finished: true,
            stream_finished: false,
        }
    }
}

struct AsyncStreamThreadReader {
    receiver: Receiver<AsyncReaderBuffer>,
    owner: Arc<AsyncReaderThread>,
    current: AsyncReaderBuffer,
    current_pos: usize,
    checkpoint_finished: bool,
    stream_finished: bool,
}

enum AsyncCheckpointInfo<T> {
    Stream(Option<T>),
    Passtrough {
        file_range: std::ops::Range<u64>,
        checkpoint_data: Option<T>,
    },
}

impl AsyncStreamThreadReader {
    fn get_checkpoint_info_and_reset_reader<T: DeserializeOwned>(
        &mut self,
    ) -> Option<AsyncCheckpointInfo<T>> {
        assert!(self.checkpoint_finished);

        if self.stream_finished {
            return None;
        }

        match &self.current {
            AsyncReaderBuffer::Closed => {
                self.stream_finished = true;
                None
            }
            AsyncReaderBuffer::Passtrough {
                file_range,
                checkpoint_data,
            } => {
                let info = AsyncCheckpointInfo::Passtrough {
                    checkpoint_data: checkpoint_data.as_ref().map(|data| {
                        bincode::deserialize(data).expect("Failed to deserialize checkpoint data")
                    }),
                    file_range: file_range.clone(),
                };

                // This buffer is now used, change it
                self.current = self.receiver.recv().unwrap();

                Some(info)
            }
            AsyncReaderBuffer::Decompressed {
                checkpoint_data, ..
            } => {
                self.checkpoint_finished = false;
                Some(AsyncCheckpointInfo::Stream(checkpoint_data.as_ref().map(
                    |data| {
                        bincode::deserialize(data).expect("Failed to deserialize checkpoint data")
                    },
                )))
            }
        }
    }
}

impl Read for AsyncStreamThreadReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            if self.checkpoint_finished {
                return Ok(bytes_read);
            }

            match &self.current {
                AsyncReaderBuffer::Closed => {
                    self.checkpoint_finished = true;
                    return Ok(bytes_read);
                }
                AsyncReaderBuffer::Passtrough { .. } => unreachable!(),
                AsyncReaderBuffer::Decompressed { data, .. } => {
                    if self.current_pos == data.len() {
                        if let Some(buffer) =
                            std::mem::replace(&mut self.current, self.receiver.recv().unwrap())
                                .into_buffer()
                        {
                            let _ = self.owner.buffers_pool.0.send(buffer);
                        }
                        self.current_pos = 0;
                        self.checkpoint_finished = !self.current.is_continuation();
                        continue;
                    }

                    let avail = data.len() - self.current_pos;
                    let to_read = min(buf.len() - bytes_read, avail);
                    buf[bytes_read..(bytes_read + to_read)]
                        .copy_from_slice(&data[self.current_pos..(self.current_pos + to_read)]);
                    bytes_read += to_read;
                    self.current_pos += to_read;

                    if bytes_read == buf.len() {
                        return Ok(bytes_read);
                    }
                }
            }
        }
    }
}

impl Drop for AsyncStreamThreadReader {
    fn drop(&mut self) {
        assert!(matches!(self.current, AsyncReaderBuffer::Closed));
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

    fn with_opened_file<T>(&self, f: impl FnOnce(&OpenedFile) -> T) -> T {
        let tmp_file;
        let opened_file = &self.opened_file.read();
        let file = match opened_file.deref() {
            OpenedFile::NotOpened | OpenedFile::Finished => {
                tmp_file = Self::open_file(&self.path, self.compressed, RemoveFileMode::Keep, None);
                &tmp_file
            }
            file => file,
        };
        f(file)
    }

    pub fn get_data_format_info<T: DeserializeOwned>(&self) -> Option<T> {
        self.with_opened_file(|file| match file {
            OpenedFile::Plain(file) => Some(file.get_data_format_info()),
            OpenedFile::Compressed(file) => Some(file.get_data_format_info()),
            OpenedFile::NotOpened | OpenedFile::Finished => None,
        })
    }

    pub fn get_chunks_count(&self) -> usize {
        self.with_opened_file(|file| file.get_chunks_count())
    }

    pub fn get_file_size(&self) -> usize {
        self.with_opened_file(|file| match file {
            OpenedFile::Plain(file) => file.get_length(),
            OpenedFile::Compressed(file) => file.get_length(),
            OpenedFile::NotOpened | OpenedFile::Finished => 0,
        })
    }
}

impl AsyncBinaryReader {
    pub fn is_finished(&self) -> bool {
        self.opened_file.read().is_finished()
    }

    pub fn get_items_stream<S: BucketItemSerializer, const ALLOW_PASSTROUGH: bool>(
        &self,
        read_thread: Arc<AsyncReaderThread>,
        buffer: S::ReadBuffer,
        extra_buffer: S::ExtraDataBuffer,
    ) -> AsyncBinaryReaderItemsIterator<S, ALLOW_PASSTROUGH> {
        let mut opened_file = self.opened_file.read();
        if matches!(*opened_file, OpenedFile::NotOpened) {
            drop(opened_file);
            let mut writable = self.opened_file.write();
            if matches!(*writable, OpenedFile::NotOpened) {
                *writable =
                    Self::open_file(&self.path, self.compressed, self.remove_file, self.prefetch);
            }
            opened_file = RwLockWriteGuard::downgrade(writable);
        }

        let stream = read_thread.read_bucket(
            opened_file.clone(),
            if ALLOW_PASSTROUGH {
                CheckpointStrategy::Passtrough
            } else {
                CheckpointStrategy::Decompress
            },
        );
        AsyncBinaryReaderItemsIterator::<_, ALLOW_PASSTROUGH> {
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

pub struct AsyncBinaryReaderItemsIterator<S: BucketItemSerializer, const ALLOW_PASSTROUGH: bool> {
    buffer: S::ReadBuffer,
    extra_buffer: S::ExtraDataBuffer,
    stream: AsyncStreamThreadReader,
    deserializer: S,
}

pub enum AsyncBinaryReaderIteratorData<'a, S: BucketItemSerializer> {
    Stream(
        &'a mut AsyncBinaryReaderItemsIteratorCheckpoint<S, true>,
        Option<S::CheckpointData>,
    ),
    Passtrough {
        file_range: std::ops::Range<u64>,
        checkpoint_data: Option<S::CheckpointData>,
    },
}

impl<S: BucketItemSerializer> AsyncBinaryReaderItemsIterator<S, true> {
    pub fn get_next_checkpoint(&mut self) -> Option<AsyncBinaryReaderIteratorData<S>> {
        let info = self.stream.get_checkpoint_info_and_reset_reader()?;
        Some(match info {
            AsyncCheckpointInfo::Stream(data) => {
                AsyncBinaryReaderIteratorData::Stream(unsafe { std::mem::transmute(self) }, data)
            }
            AsyncCheckpointInfo::Passtrough {
                file_range,
                checkpoint_data,
            } => AsyncBinaryReaderIteratorData::Passtrough {
                file_range,
                checkpoint_data,
            },
        })
    }
}

impl<S: BucketItemSerializer> AsyncBinaryReaderItemsIterator<S, false> {
    pub fn get_next_checkpoint(
        &mut self,
    ) -> Option<(
        &mut AsyncBinaryReaderItemsIteratorCheckpoint<S, false>,
        Option<S::CheckpointData>,
    )> {
        let info = self.stream.get_checkpoint_info_and_reset_reader()?;
        Some(match info {
            AsyncCheckpointInfo::Stream(data) => (unsafe { std::mem::transmute(self) }, data),
            AsyncCheckpointInfo::Passtrough { .. } => unreachable!(),
        })
    }
}

#[repr(transparent)]
pub struct AsyncBinaryReaderItemsIteratorCheckpoint<
    S: BucketItemSerializer,
    const ALLOW_PASSTROUGH: bool,
>(AsyncBinaryReaderItemsIterator<S, ALLOW_PASSTROUGH>);

impl<S: BucketItemSerializer, const ALLOW_PASSTROUGH: bool>
    AsyncBinaryReaderItemsIteratorCheckpoint<S, ALLOW_PASSTROUGH>
{
    pub fn next(&mut self) -> Option<(S::ReadType<'_>, &mut S::ExtraDataBuffer)> {
        let item = self.0.deserializer.read_from(
            &mut self.0.stream,
            &mut self.0.buffer,
            &mut self.0.extra_buffer,
        )?;
        Some((item, &mut self.0.extra_buffer))
    }
}
