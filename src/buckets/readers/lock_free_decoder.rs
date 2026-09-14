use crate::buckets::readers::binary_reader::ChunkDecoder;
use crate::buckets::writers::lock_free_binary_writer::LOCK_FREE_BUCKET_MAGIC;
use crate::memory_fs::file::reader::FileReader;
use crate::utils::vec_reader::VecReader;
use std::io::{Read, Take};

pub struct LockFreeStreamDecoder;

/// Smaller than the compressed decoder's, because this stream is often backed by
/// memory and a reader is built per chunk: it only has to be wide enough to
/// amortize the lock a `FileReader` takes on every read.
const DEFAULT_BUFFER_SIZE: usize = 1024 * 64;

impl ChunkDecoder for LockFreeStreamDecoder {
    const MAGIC_HEADER: &'static [u8; 16] = LOCK_FREE_BUCKET_MAGIC;
    type ReaderType = VecReader<Take<FileReader>>;

    fn decode_stream(reader: FileReader, size: u64) -> Self::ReaderType {
        VecReader::new(DEFAULT_BUFFER_SIZE, reader.take(size))
    }

    fn dispose_stream(stream: Self::ReaderType) -> FileReader {
        stream.into_inner().into_inner()
    }
}
