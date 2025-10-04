use std::{
    cell::UnsafeCell,
    fs::{self, File},
    io::{self, Write},
    os::fd::AsRawFd,
    path::Path,
};

use libc::{c_void, pread, pwrite};

use crate::storage::PageNumber;

pub type BlockNumber = u32;

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

/// Wrapper to simplify working with page like structures on disk.
#[derive(Debug)]
pub struct BlockIO<I> {
    io: UnsafeCell<I>,
    block_size: usize,
    header_size: usize,
}

impl<I> BlockIO<I> {
    pub fn new(io: I, block_size: usize, header_size: usize) -> Self {
        Self {
            io: UnsafeCell::new(io),
            block_size,
            header_size,
        }
    }

    pub fn get_io(&self) -> &mut I {
        unsafe { self.io.get().as_mut().unwrap() }
    }
}

impl<I: IO> BlockIO<I> {
    pub fn raw_read(&self, offset: usize, buffer: &mut [u8]) -> io::Result<usize> {
        self.get_io().pread(offset, buffer)
    }

    /// Reads block with given number. Includes header offset. This operation is atomic. `block_number starts at 1`.
    pub fn read(&self, block_number: BlockNumber, buffer: &mut [u8]) -> io::Result<usize> {
        assert!(block_number > 0, "block number must be grater than 0.");
        let block_number = (block_number - 1) as usize;
        let offset = self.header_size + block_number * self.block_size;
        self.raw_read(offset, buffer)
    }

    /// Reads header from beginning of a file. Note that buffer size must match header size. This operation is atomic.
    pub fn read_header(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.raw_read(0, buffer)
    }
}

impl<I: IO> BlockIO<I> {
    pub fn raw_write(&self, offset: usize, buffer: &[u8]) -> io::Result<usize> {
        self.get_io().pwrite(offset, buffer)
    }

    /// Writes block at given number. Includes header offset. This operation is atomic. `page_number starts at 1`.
    pub fn write(&self, block_number: BlockNumber, buffer: &[u8]) -> io::Result<usize> {
        assert!(block_number > 0, "block number must be grater than 0.");
        let block_number = (block_number - 1) as usize;
        let offset = self.header_size + block_number as usize * self.block_size;
        self.raw_write(offset, buffer)
    }

    /// Writes header at the beginning of a file. Note that buffer size must match header size. This operation is atomic.
    pub fn write_header(&self, buffer: &[u8]) -> io::Result<usize> {
        self.raw_write(0, buffer)
    }
}

impl<I: Write> BlockIO<I> {
    /// Flush buffered contents.
    ///
    /// This does not guarantee that the contents reach the filesystem. Use
    /// [`Self::sync`] after flushing.
    pub fn flush(&self) -> io::Result<()> {
        self.get_io().flush()
    }
}

impl<I: FileOps> BlockIO<I> {
    /// See [`Sync`] for details.
    pub fn sync(&self) -> io::Result<()> {
        self.get_io().sync()
    }
}

impl BlockIO<File> {
    /// Returns size of file inside wrapper in **bytes**.
    pub fn size(&self) -> io::Result<usize> {
        let meta = self.get_io().metadata()?;
        Ok(meta.len() as usize)
    }

    /// Returns size of file inside wrapper in **blocks**.
    pub fn size_in_blocks(&self) -> io::Result<usize> {
        Ok((self.size()? - self.header_size) / self.block_size)
    }
}
