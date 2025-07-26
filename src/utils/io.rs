use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    path::Path,
};

use libc::{c_void, pread, pwrite};

use crate::{os::DISK_BLOCK_SIZE, storage::PageNumber};

pub trait FileOps {
    /// Creates file in filesystem at the given path.
    ///
    /// If file already exists it is truncated and if parent
    /// directories are not present the will be created as well.
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    /// Opens file "as is" no truncate
    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    /// Removes file at given path
    fn remove(path: impl AsRef<Path>) -> io::Result<()>;

    /// Truncates file to 0 length
    fn truncate(&mut self) -> io::Result<()>;

    /// Attempts to persist data on disk    
    fn sync(&self) -> io::Result<()>;
}

impl FileOps for File {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        File::options().read(true).write(false).open(path)
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn truncate(&mut self) -> io::Result<()> {
        self.set_len(0)
    }

    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

pub trait IO {
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize>;
    fn pwrite(&self, offset: usize, buf: &[u8]) -> io::Result<usize>;
}

impl IO for File {
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        let read = unsafe {
            pread(
                self.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                offset as i64,
            )
        };
        if read == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(read as usize)
        }
    }

    fn pwrite(&self, offset: usize, buf: &[u8]) -> io::Result<usize> {
        let written = unsafe {
            pwrite(
                self.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                buf.len(),
                offset as i64,
            )
        };
        if written == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }
}

/// Wrapper to simplify working with page like structures on disk
#[derive(Debug)]
pub struct BlockIO<I> {
    io: I,
    pub page_size: usize,
    header_size: usize,
}

impl<I> BlockIO<I> {
    pub fn new(io: I, page_size: usize, header_size: usize) -> Self {
        Self {
            io,
            page_size,
            header_size,
        }
    }
}

impl<I: IO> BlockIO<I> {
    pub fn raw_read(&mut self, offset: usize, buffer: &mut [u8]) -> io::Result<usize> {
        self.io.pread(offset, buffer)
    }

    /// Reads page with given number. Includes header offset.
    pub fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> io::Result<usize> {
        let page_number = page_number as usize;
        let offset = self.header_size + page_number * self.page_size;
        self.raw_read(offset, buffer)
    }

    /// Reads header from beginning of a file. Note that buffer size must match header size.
    pub fn read_header(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.raw_read(0, buffer)
    }
}

impl<I: IO> BlockIO<I> {
    pub fn raw_write(&mut self, offset: usize, buffer: &[u8]) -> io::Result<usize> {
        self.io.pwrite(offset, buffer)
    }

    /// Writes page at given number. Includes header offset.
    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> io::Result<usize> {
        let offset = self.header_size + page_number as usize * self.page_size;
        self.raw_write(offset, buffer)
    }

    /// Writes header at the beginning of a file. Note that buffer size must match header size.
    pub fn write_header(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.raw_write(0, buffer)
    }
}

impl<I: Write> BlockIO<I> {
    /// Flush buffered contents.
    ///
    /// This does not guarantee that the contents reach the filesystem. Use
    /// [`Self::sync`] after flushing.
    pub fn flush(&mut self) -> io::Result<()> {
        self.io.flush()
    }
}

impl<I: FileOps> BlockIO<I> {
    /// See [`Sync`] for details.
    pub fn sync(&self) -> io::Result<()> {
        self.io.sync()
    }
}
