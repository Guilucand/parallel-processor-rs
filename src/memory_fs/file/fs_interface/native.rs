use std::{
    fs::{remove_file, File, OpenOptions},
    io::{Seek, Write},
    path::{Path, PathBuf},
};

use filebuffer::FileBuffer;
use parking_lot::RwLock;

use super::{FileReaderInterface, FileWriterInterface, FsInterface};

// pub const O_DIRECT: i32 = 0x4000;

pub struct NativeFs {
    base_dir: RwLock<Option<PathBuf>>,
}

pub struct NativeFileWriter(File);
pub struct NativeFileReader(FileBuffer);

impl NativeFs {
    pub const fn new() -> Self {
        Self {
            base_dir: RwLock::new(None),
        }
    }
}

impl FsInterface for NativeFs {
    type FileWriter = NativeFileWriter;
    type FileReader = NativeFileReader;

    fn set_base_dir(&self, base_dir: &Path) {
        *self.base_dir.write() = Some(base_dir.to_path_buf());
    }

    fn new_file(&self, name: &str) -> Self::FileWriter {
        let path = self.base_dir.read().as_ref().unwrap().join(name);

        // Remove the file if it existed from a previous run
        let _ = remove_file(&path);

        NativeFileWriter(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(false)
                // .custom_flags(O_DIRECT)
                .open(path)
                .unwrap(),
        )
    }

    fn open_file(&self, name: &str) -> Result<Self::FileReader, String> {
        Ok(NativeFileReader(
            FileBuffer::open(self.base_dir.read().as_ref().unwrap().join(name))
                .map_err(|e| e.to_string())?,
        ))
    }

    fn delete_file(&self, name: &str) {
        let _ = remove_file(self.base_dir.read().as_ref().unwrap().join(name));
    }
}

impl FileWriterInterface for NativeFileWriter {
    fn write_data(&mut self, data: &[u8]) {
        self.0.write_all(data).unwrap()
    }

    fn flush(&mut self) {
        self.0.flush().unwrap()
    }

    fn get_position(&mut self) -> u64 {
        self.0.stream_position().unwrap()
    }

    fn write_at_start(&mut self, data: &[u8]) {
        let position = self.get_position();
        self.0.seek(std::io::SeekFrom::Start(0)).unwrap();
        self.0.write_all(data).unwrap();
        self.0.seek(std::io::SeekFrom::Start(position)).unwrap();
    }
}

impl FileReaderInterface for NativeFileReader {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn prefetch(&self, offset: usize, size: usize) {
        self.0.prefetch(offset, size)
    }

    unsafe fn get_mmap_at(&self, offset: usize) -> *const u8 {
        self.0.as_ptr().add(offset)
    }
}
