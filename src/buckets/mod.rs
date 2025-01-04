use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub mod bucket_writer;
pub mod concurrent;
pub mod readers;
pub mod single;
pub mod writers;

pub trait LockFreeBucket {
    type InitData: Clone;

    fn new(path: &Path, data: &Self::InitData, index: usize) -> Self;
    fn write_data(&self, bytes: &[u8]);
    fn get_path(&self) -> PathBuf;
    fn finalize(self);
}

pub struct MultiThreadBuckets<B: LockFreeBucket> {
    active_buckets: Vec<ArcSwap<(AtomicU64, B)>>,
    stored_buckets: Mutex<Vec<Vec<PathBuf>>>,
    disk_usage: AtomicU64,
    active_disk_usage_limit: Option<NonZeroU64>,
    bucket_count_lock: Mutex<usize>,
    base_path: Option<PathBuf>,
    init_data: Option<B::InitData>,
}

impl<B: LockFreeBucket> MultiThreadBuckets<B> {
    pub const EMPTY: Self = Self {
        active_buckets: vec![],
        stored_buckets: Mutex::new(vec![]),
        disk_usage: AtomicU64::new(0),
        active_disk_usage_limit: None,
        bucket_count_lock: Mutex::new(0),
        base_path: None,
        init_data: None,
    };

    pub fn new(
        size: usize,
        path: PathBuf,
        active_disk_usage_limit: Option<u64>,
        init_data: &B::InitData,
    ) -> MultiThreadBuckets<B> {
        let mut buckets = Vec::with_capacity(size);

        for i in 0..size {
            buckets.push(ArcSwap::from_pointee((
                AtomicU64::new(0),
                B::new(&path, init_data, i),
            )));
        }
        MultiThreadBuckets {
            active_buckets: buckets,
            stored_buckets: Mutex::new(vec![vec![]; size]),
            disk_usage: AtomicU64::new(0),
            active_disk_usage_limit: active_disk_usage_limit.map(NonZeroU64::new).flatten(),
            bucket_count_lock: Mutex::new(size),
            base_path: Some(path),
            init_data: Some(init_data.clone()),
        }
    }

    pub fn into_buckets(mut self) -> impl Iterator<Item = B> {
        assert!(
            self.stored_buckets
                .lock()
                .iter()
                .all(|bucket| bucket.is_empty())
                && self.active_disk_usage_limit.is_none()
        );
        let buckets = std::mem::take(&mut self.active_buckets);
        buckets
            .into_iter()
            .map(|bucket| Arc::into_inner(bucket.into_inner()).unwrap().1)
    }

    pub fn get_path(&self, bucket: u16) -> PathBuf {
        self.active_buckets[bucket as usize].load().1.get_path()
    }

    pub fn add_data(&self, index: u16, data: &[u8]) {
        let bucket_guard = self.active_buckets[index as usize].load();
        bucket_guard.1.write_data(data);

        // Add the data size to both the bucket and the global disk usages
        bucket_guard
            .0
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let mut disk_usage = self
            .disk_usage
            .fetch_add(data.len() as u64, Ordering::Relaxed)
            + data.len() as u64;

        drop(bucket_guard);

        // If the disk usage limit is set, check if the disk usage is not exceeded
        if let Some(max_usage) = self.active_disk_usage_limit {
            while disk_usage > max_usage.get() {
                let mut buckets_count = self.bucket_count_lock.lock();
                // Take the largest bucket and add it to the stored buckets
                let swap_bucket_index = self
                    .active_buckets
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, bucket)| bucket.load().0.load(Ordering::Relaxed))
                    .map(|(i, _bucket)| i)
                    .unwrap();

                let mut stored_bucket = self.active_buckets[swap_bucket_index].swap(Arc::new((
                    AtomicU64::new(0),
                    B::new(
                        &self.base_path.as_deref().unwrap(),
                        &self.init_data.as_ref().unwrap(),
                        *buckets_count,
                    ),
                )));

                let (bucket_usage, stored_bucket) = loop {
                    // Wait for the bucket to end all the pending writes before finalizing it
                    match Arc::try_unwrap(stored_bucket) {
                        Ok(bucket) => break bucket,
                        Err(waiting_arc) => {
                            stored_bucket = waiting_arc;
                            std::hint::spin_loop();
                        }
                    }
                };
                let bucket_usage = bucket_usage.into_inner();

                // Add the bucket to the stored buckets and clear its active usage
                disk_usage =
                    self.disk_usage.fetch_sub(bucket_usage, Ordering::Relaxed) - bucket_usage;
                self.stored_buckets.lock()[swap_bucket_index].push(stored_bucket.get_path());
                stored_bucket.finalize();

                *buckets_count += 1;
            }
        }
    }

    pub fn count(&self) -> usize {
        self.active_buckets.len()
    }

    pub fn finalize_single(self: Arc<Self>) -> Vec<PathBuf> {
        assert!(self.active_disk_usage_limit.is_none());
        let buckets = self.finalize();
        buckets
            .into_iter()
            .map(|mut bucket| {
                assert!(bucket.len() == 1);
                bucket.pop().unwrap()
            })
            .collect()
    }

    pub fn finalize(self: Arc<Self>) -> Vec<Vec<PathBuf>> {
        let mut self_ = Arc::try_unwrap(self)
            .unwrap_or_else(|_| panic!("Cannot take full ownership of multi thread buckets!"));

        let mut stored_buckets = self_.stored_buckets.lock();

        self_
            .active_buckets
            .drain(..)
            .zip(stored_buckets.drain(..))
            .map(|(bucket, mut stored)| {
                let bucket = Arc::into_inner(bucket.into_inner()).unwrap();
                stored.push(bucket.1.get_path());
                bucket.1.finalize();
                stored
            })
            .collect()
    }
}

impl<B: LockFreeBucket> Drop for MultiThreadBuckets<B> {
    fn drop(&mut self) {
        self.active_buckets.drain(..).for_each(|bucket| {
            let bucket = Arc::into_inner(bucket.into_inner()).unwrap();
            bucket.1.finalize();
        });
    }
}

unsafe impl<B: LockFreeBucket> Send for MultiThreadBuckets<B> {}

unsafe impl<B: LockFreeBucket> Sync for MultiThreadBuckets<B> {}
