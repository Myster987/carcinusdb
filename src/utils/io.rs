use std::{
    cell::UnsafeCell,
    fs::{self, File},
    io::{self, IoSlice, Seek, Write},
    os::fd::AsRawFd,
    path::Path,
};

use libc::c_void;

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

    #[cfg(target_os = "linux")]
    fn sync(&self) -> io::Result<()> {
        let fd = self.as_raw_fd();
        let res = unsafe { libc::fsync(fd) };
        if res == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

pub trait IO {
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize>;
    fn pwrite(&self, offset: usize, buf: &[u8]) -> io::Result<usize>;
    fn pwrite_vec(&self, offset: usize, buf: &mut [IoSlice<'_>]) -> io::Result<usize>;
    fn truncate_beginning(&mut self, bytes_to_remove: u64, header_size: usize) -> io::Result<()>;
}

impl IO for File {
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        let read = unsafe {
            libc::pread(
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
            libc::pwrite(
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

    fn pwrite_vec(&self, mut offset: usize, buf: &mut [IoSlice<'_>]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total_written = 0;

        let mut iovecs = buf;

        while !iovecs.is_empty() {
            let written = unsafe {
                libc::pwritev(
                    self.as_raw_fd(),
                    iovecs.as_ptr().cast(),
                    iovecs.len() as libc::c_int,
                    offset as libc::off_t,
                )
            };

            if written < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(err);
                }
            }

            let mut written = written as usize;
            total_written += written;
            offset += written;

            // skip written iovecs
            while !iovecs.is_empty() && written >= iovecs[0].len() {
                written -= iovecs[0].len();
                iovecs = &mut iovecs[1..];
            }

            // advance iovec if part of it was written
            if !iovecs.is_empty() && written > 0 {
                iovecs[0].advance(written);
            }
        }

        Ok(total_written)
    }

    /// Truncates beginning of a file (skips header). `bytes_to_remove` is deleted and rest is
    /// moved to beginning using [`libc::copy_file_range`].
    ///
    /// # Example
    /// ```text
    /// Block size: 10 bytes
    /// Bytes to remove: 30
    ///
    /// Before:
    /// +--------+---------+---------+---------+---------+---------+
    /// | Header | Block 1 | Block 2 | Block 3 | Block 4 | Block 5 |
    /// +--------+---------+---------+---------+---------+---------+
    ///
    /// After (Block 1, 2 and 3 were removed):
    /// +--------+---------+---------+
    /// | Header | Block 4 | Block 5 |
    /// +--------+---------+---------+
    /// ```
    fn truncate_beginning(&mut self, bytes_to_remove: u64, header_size: usize) -> io::Result<()> {
        let fd = self.as_raw_fd();
        let metadata = self.metadata()?;
        let file_size = metadata.len();

        if bytes_to_remove + header_size as u64 >= file_size {
            unsafe {
                if libc::ftruncate(fd, header_size as libc::off_t) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }

            return Ok(());
        }

        let remaining = file_size - bytes_to_remove - header_size as u64;

        let mut off_in = (header_size as u64 + bytes_to_remove) as libc::off64_t;
        let mut off_out = header_size as libc::off64_t;

        let mut copied = 0;

        while copied < remaining {
            let chunk = unsafe {
                libc::copy_file_range(
                    fd,
                    &mut off_in,
                    fd,
                    &mut off_out,
                    remaining as libc::size_t,
                    0,
                )
            };

            if chunk < 0 {
                return Err(io::Error::last_os_error());
            }

            copied += chunk as u64;
        }

        let new_size = header_size as u64 + remaining;

        unsafe {
            if libc::ftruncate(fd, new_size as libc::off_t) != 0 {
                return Err(io::Error::last_os_error());
            }
        }

        self.seek(io::SeekFrom::Start(0))?;
        Ok(())
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

    pub fn write_vectored(
        &self,
        block_number: BlockNumber,
        buffers: &mut [IoSlice],
    ) -> io::Result<usize> {
        assert!(block_number > 0, "block number must be grater than 0.");
        let block_number = (block_number - 1) as usize;
        let offset = self.header_size + block_number as usize * self.block_size;
        self.get_io().pwrite_vec(offset, buffers)
    }

    /// Writes header at the beginning of a file. Note that buffer size must match header size. This operation is atomic.
    pub fn write_header(&self, buffer: &[u8]) -> io::Result<usize> {
        self.raw_write(0, buffer)
    }

    /// Removes first `up_to_block_numer` (inclusive) from file. Shifts rest to
    /// beginning of a file. For more information go to `IO::truncate_beginning`.
    ///
    /// # Example
    /// ```text
    /// File before: [Header, Block 1, Block 2, Block 3, Block 4, Block 5]
    ///
    /// up_to_block_numer = 3
    ///
    /// File after: [Header, Block 4, Block 5]
    /// ```
    pub fn truncate_beginning(&self, up_to_block_number: BlockNumber) -> io::Result<()> {
        assert!(
            up_to_block_number > 0,
            "block number, up to which file will be truncated, must be grater than 0."
        );

        let bytes_to_remove = (up_to_block_number as usize * self.block_size) as u64;

        self.get_io()
            .truncate_beginning(bytes_to_remove, self.header_size)
    }
}

impl<I: Write> BlockIO<I> {
    /// Flush buffered contents to kernel.
    ///
    /// This does not guarantee that the contents reach the filesystem.
    /// Use [`Self::sync`] after flushing.
    pub fn flush(&self) -> io::Result<()> {
        self.get_io().flush()
    }
}

impl<I: FileOps> BlockIO<I> {
    /// Makes syscall to kernel to write **flushed** buffers to disk.
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
