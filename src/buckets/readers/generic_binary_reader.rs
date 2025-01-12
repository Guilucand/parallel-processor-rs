use crate::buckets::bucket_writer::BucketItemSerializer;
use crate::buckets::readers::BucketReader;
use crate::buckets::writers::{BucketCheckpoints, BucketHeader};
use crate::memory_fs::file::reader::{FileRangeReference, FileReader};
use crate::memory_fs::{MemoryFs, RemoveFileMode};
use desse::Desse;
use desse::DesseSized;
use replace_with::replace_with_or_abort;
use serde::de::DeserializeOwned;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::async_binary_reader::AllowedCheckpointStrategy;

pub trait ChunkDecoder {
    const MAGIC_HEADER: &'static [u8; 16];
    type ReaderType: Read;
    fn decode_stream(reader: FileReader, size: u64) -> Self::ReaderType;
    fn dispose_stream(stream: Self::ReaderType) -> FileReader;
}

pub struct GenericChunkedBinaryReader<D: ChunkDecoder> {
    remove_file: RemoveFileMode,
    sequential_reader: SequentialReader<D>,
    parallel_reader: FileReader,
    parallel_index: AtomicU64,
    file_path: PathBuf,
    format_data_info: Vec<u8>,
}

pub enum ChunkReader<T, R> {
    Reader(R, Option<T>),
    Passtrough {
        file_range: FileRangeReference,
        data: Option<T>,
    },
}

pub enum DecodeItemsStatus<T> {
    Decompressed,
    Passtrough {
        file_range: FileRangeReference,
        data: Option<T>,
    },
}

unsafe impl<D: ChunkDecoder> Sync for GenericChunkedBinaryReader<D> {}

struct SequentialReader<D: ChunkDecoder> {
    reader: D::ReaderType,
    index: BucketCheckpoints,
    last_byte_position: u64,
    index_position: u64,
}

impl<D: ChunkDecoder> SequentialReader<D> {
    fn get_chunk_size(
        checkpoints: &BucketCheckpoints,
        last_byte_position: u64,
        index: usize,
    ) -> u64 {
        if checkpoints.index.len() > (index + 1) as usize {
            checkpoints.index[(index + 1) as usize].offset
                - checkpoints.index[index as usize].offset
        } else {
            last_byte_position - checkpoints.index[index as usize].offset
        }
    }
}

impl<D: ChunkDecoder> Read for SequentialReader<D> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.reader.read(buf) {
                Ok(read) => {
                    if read != 0 {
                        return Ok(read);
                    }
                }
                Err(err) => {
                    return Err(err);
                }
            }
            self.index_position += 1;

            if self.index_position >= self.index.index.len() as u64 {
                return Ok(0);
            }

            replace_with_or_abort(&mut self.reader, |reader| {
                let mut file = D::dispose_stream(reader);
                // This assert sometimes fails as the lz4 library can buffer more data that is then discarded on drop
                // assert_eq!(
                //     file.stream_position().unwrap(),
                //     self.index.index[self.index_position as usize]
                // );
                file.seek(SeekFrom::Start(
                    self.index.index[self.index_position as usize].offset,
                ))
                .unwrap();
                let size = SequentialReader::<D>::get_chunk_size(
                    &self.index,
                    self.last_byte_position,
                    self.index_position as usize,
                );
                D::decode_stream(file, size)
            });
        }
    }
}

impl<D: ChunkDecoder> GenericChunkedBinaryReader<D> {
    pub fn new(
        name: impl AsRef<Path>,
        remove_file: RemoveFileMode,
        prefetch_amount: Option<usize>,
    ) -> Self {
        let mut file = FileReader::open(&name, prefetch_amount)
            .unwrap_or_else(|| panic!("Cannot open file {}", name.as_ref().display()));

        let mut header_buffer = [0; BucketHeader::SIZE];
        file.read_exact(&mut header_buffer)
            .unwrap_or_else(|_| panic!("File {} is corrupted", name.as_ref().display()));

        let header: BucketHeader = BucketHeader::deserialize_from(&header_buffer);
        assert_eq!(&header.magic, D::MAGIC_HEADER);

        file.seek(SeekFrom::Start(header.index_offset)).unwrap();
        let index: BucketCheckpoints = bincode::deserialize_from(&mut file).unwrap();

        // crate::log_info!(
        //     "Index: {} for {}",
        //     index.index.len(),
        //     name.as_ref().display()
        // );

        file.seek(SeekFrom::Start(index.index[0].offset)).unwrap();

        let size = SequentialReader::<D>::get_chunk_size(&index, header.index_offset, 0);

        Self {
            sequential_reader: SequentialReader {
                reader: D::decode_stream(file, size),
                index,
                last_byte_position: header.index_offset,
                index_position: 0,
            },
            parallel_reader: FileReader::open(&name, prefetch_amount).unwrap(),
            parallel_index: AtomicU64::new(0),
            remove_file,
            file_path: name.as_ref().to_path_buf(),
            format_data_info: header.data_format_info.to_vec(),
        }
    }

    pub fn get_data_format_info<T: DeserializeOwned>(&self) -> T {
        bincode::deserialize(&self.format_data_info).unwrap()
    }

    pub fn get_length(&self) -> usize {
        self.parallel_reader.total_file_size()
    }

    pub fn get_chunks_count(&self) -> usize {
        self.sequential_reader.index.index.len()
    }

    pub fn is_finished(&self) -> bool {
        self.parallel_index.load(Ordering::Relaxed) as usize
            >= self.sequential_reader.index.index.len()
    }

    pub fn get_single_stream<'a>(&'a mut self) -> impl Read + 'a {
        &mut self.sequential_reader
    }

    pub fn get_read_parallel_stream_with_chunk_type<T: DeserializeOwned>(
        &self,
        allowed_strategy: AllowedCheckpointStrategy<[u8]>,
    ) -> Option<ChunkReader<T, D::ReaderType>> {
        match self.get_read_parallel_stream(allowed_strategy) {
            None => None,
            Some(ChunkReader::Reader(stream, data)) => Some(ChunkReader::Reader(
                stream,
                data.map(|data| bincode::deserialize(&data).unwrap()),
            )),
            Some(ChunkReader::Passtrough { file_range, data }) => Some(ChunkReader::Passtrough {
                file_range,
                data: data.map(|data| bincode::deserialize(&data).unwrap()),
            }),
        }
    }

    pub fn get_read_parallel_stream(
        &self,
        allowed_strategy: AllowedCheckpointStrategy<[u8]>,
    ) -> Option<ChunkReader<Vec<u8>, D::ReaderType>> {
        let index = self.parallel_index.fetch_add(1, Ordering::Relaxed) as usize;

        if index >= self.sequential_reader.index.index.len() {
            return None;
        }

        let addr_start = self.sequential_reader.index.index[index].offset as usize;
        let checkpoint_data = self.sequential_reader.index.index[index].data.clone();

        let mut reader = self.parallel_reader.clone();
        reader.seek(SeekFrom::Start(addr_start as u64)).unwrap();

        let size = SequentialReader::<D>::get_chunk_size(
            &self.sequential_reader.index,
            self.sequential_reader.last_byte_position,
            index,
        );

        match allowed_strategy {
            AllowedCheckpointStrategy::DecompressOnly => Some(ChunkReader::Reader(
                D::decode_stream(reader, size),
                checkpoint_data,
            )),
            AllowedCheckpointStrategy::AllowPasstrough(checker) => {
                if checker(checkpoint_data.as_deref()) {
                    let file_range = (addr_start as u64)..(addr_start as u64 + size);
                    Some(ChunkReader::Passtrough {
                        file_range: reader.get_range_reference(file_range),
                        data: checkpoint_data,
                    })
                } else {
                    Some(ChunkReader::Reader(
                        D::decode_stream(reader, size),
                        checkpoint_data,
                    ))
                }
            }
        }
    }

    pub fn decode_bucket_items_parallel<
        S: BucketItemSerializer,
        F: for<'a> FnMut(S::ReadType<'a>, &mut S::ExtraDataBuffer, Option<&S::CheckpointData>),
    >(
        &self,
        mut buffer: S::ReadBuffer,
        mut extra_buffer: S::ExtraDataBuffer,
        allowed_strategy: AllowedCheckpointStrategy<[u8]>,
        mut func: F,
    ) -> Option<DecodeItemsStatus<S::CheckpointData>> {
        let stream = match self
            .get_read_parallel_stream_with_chunk_type::<S::CheckpointData>(allowed_strategy)
        {
            None => return None,
            Some(stream) => stream,
        };

        let mut deserializer = S::new();

        match stream {
            ChunkReader::Reader(mut stream, data) => {
                while let Some(el) =
                    deserializer.read_from(&mut stream, &mut buffer, &mut extra_buffer)
                {
                    func(el, &mut extra_buffer, data.as_ref());
                }
                Some(DecodeItemsStatus::Decompressed)
            }
            ChunkReader::Passtrough { file_range, data } => {
                Some(DecodeItemsStatus::Passtrough { file_range, data })
            }
        }
    }
}

impl<D: ChunkDecoder> BucketReader for GenericChunkedBinaryReader<D> {
    fn decode_all_bucket_items<
        S: BucketItemSerializer,
        F: for<'a> FnMut(S::ReadType<'a>, &mut S::ExtraDataBuffer),
    >(
        mut self,
        mut buffer: S::ReadBuffer,
        extra_buffer: &mut S::ExtraDataBuffer,
        mut func: F,
    ) {
        let mut stream = self.get_single_stream();

        let mut deserializer = S::new();
        while let Some(el) = deserializer.read_from(&mut stream, &mut buffer, extra_buffer) {
            func(el, extra_buffer);
        }
    }

    fn get_name(&self) -> PathBuf {
        self.file_path.clone()
    }
}

impl<D: ChunkDecoder> Drop for GenericChunkedBinaryReader<D> {
    fn drop(&mut self) {
        MemoryFs::remove_file(&self.file_path, self.remove_file).unwrap();
    }
}
