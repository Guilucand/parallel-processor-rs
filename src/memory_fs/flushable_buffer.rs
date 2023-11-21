use crate::memory_fs::file::internal::FileChunk;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use parking_lot::{Mutex, RawRwLock};
use std::sync::Arc;

use super::file::fs_interface::DefaultWriteFileInterface;

pub struct FlushableItem {
    pub underlying_file: Arc<(String, Mutex<DefaultWriteFileInterface>)>,
    pub mode: FileFlushMode,
}

pub enum FileFlushMode {
    Append {
        chunk: ArcRwLockWriteGuard<RawRwLock, FileChunk>,
    },
    // WriteAt {
    //     buffer: AllocatedChunk,
    //     offset: u64,
    // },
}
