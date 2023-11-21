use std::path::Path;

mod native;
pub trait FileWriterInterface {
    fn write_data(&mut self, data: &[u8]);
    fn flush(&mut self);
    fn get_position(&mut self) -> u64;
    fn write_at_start(&mut self, data: &[u8]);
}

pub trait FileReaderInterface {
    fn len(&self) -> usize;
    fn prefetch(&self, offset: usize, size: usize);
    unsafe fn get_mmap_at(&self, offset: usize) -> *const u8;
    // fn read_data(&mut self, data: &mut [u8]) -> usize;
}

pub trait FsInterface {
    type FileReader;
    type FileWriter;
    fn set_base_dir(&self, base_dir: &Path);
    fn new_file(&self, name: &str) -> Self::FileWriter;
    fn open_file(&self, name: &str) -> Result<Self::FileReader, String>;
    fn delete_file(&self, name: &str);
}

pub type DefaultFsInterface = native::NativeFs;
pub type DefaultWriteFileInterface = <DefaultFsInterface as FsInterface>::FileWriter;
pub type DefaultReadFileInterface = <DefaultFsInterface as FsInterface>::FileReader;
