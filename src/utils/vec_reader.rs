use std::cmp::min;
use std::io::{self, Read};

pub struct VecReader<R: Read> {
    reader: R,
    inner: VecReaderInner,
}

#[derive(Clone)]
pub struct VecReaderInner {
    vec: Vec<u8>,
    fill: usize,
    pos: usize,
    stream_ended: bool,
}

impl<R: Read> VecReader<R> {
    pub fn new(capacity: usize, reader: R) -> VecReader<R> {
        VecReader {
            reader,
            inner: VecReaderInner::new(capacity),
        }
    }

    #[inline]
    pub fn read_bytes(&mut self, slice: &mut [u8]) -> usize {
        self.inner.read_bytes(slice, |buf| self.reader.read(buf))
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl VecReaderInner {
    pub fn new(capacity: usize) -> VecReaderInner {
        let mut vec = Vec::with_capacity(capacity);
        unsafe {
            vec.set_len(capacity);
        }
        VecReaderInner {
            vec,
            fill: 0,
            pos: 0,
            stream_ended: false,
        }
    }

    pub fn discard(&mut self, amount: usize) -> usize {
        let fill_amount = self.fill - self.pos;
        let discard_amount = fill_amount.min(amount);
        self.pos += discard_amount;
        amount - discard_amount
    }

    pub fn reset(&mut self) {
        self.fill = 0;
        self.pos = 0;
        self.stream_ended = false;
    }

    #[inline]
    fn update_buffer(&mut self, mut read_fn: impl FnMut(&mut [u8]) -> io::Result<usize>) {
        self.fill = match read_fn(&mut self.vec[..]) {
            Ok(fill) => fill,
            Err(_) => 0,
        };
        self.stream_ended = self.fill == 0;
        self.pos = 0;
    }

    #[inline]
    pub fn read_bytes(
        &mut self,
        slice: &mut [u8],
        mut read_fn: impl FnMut(&mut [u8]) -> io::Result<usize>,
    ) -> usize {
        let mut offset = 0;

        while offset < slice.len() {
            if self.fill == self.pos {
                self.update_buffer(&mut read_fn);

                if self.fill == self.pos {
                    return offset;
                }
            }

            let amount = min(slice.len() - offset, self.fill - self.pos);

            unsafe {
                std::ptr::copy(
                    self.vec.as_ptr().add(self.pos),
                    slice.as_mut_ptr().add(offset),
                    amount,
                );
            }

            self.pos += amount;
            offset += amount;
        }
        offset
    }
}

impl<R: Read> Read for VecReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.read_bytes(buf))
    }

    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let size = self.read_bytes(buf);
        if size == buf.len() {
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))
        }
    }
}
