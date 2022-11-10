use crate::buckets::bucket_writer::BucketItem;
use std::path::PathBuf;

pub mod async_binary_reader;
pub mod compressed_binary_reader;
pub mod generic_binary_reader;
pub mod lock_free_binary_reader;
pub mod unbuffered_compressed_binary_reader;

pub trait BucketReader {
    fn decode_all_bucket_items<
        E: BucketItem,
        F: for<'a> FnMut(E::ReadType<'a>, &mut E::ExtraDataBuffer),
    >(
        self,
        buffer: E::ReadBuffer,
        extra_buffer: &mut E::ExtraDataBuffer,
        func: F,
    );

    fn get_name(&self) -> PathBuf;
}
