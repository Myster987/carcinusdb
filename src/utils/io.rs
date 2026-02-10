use std::{
    cell::UnsafeCell,
    fs::{self, File},
    io::{self, IoSlice, Seek, Write},
    os::fd::AsRawFd,
    path::Path,
    sync::LazyLock,
};

use libc::c_void;

use crate::utils::buffer::Buffer;

pub type BlockNumber = u32;

pub const MAX_VECTORED_IO_BUFFERS: LazyLock<usize> =
    LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_IOV_MAX) } as usize);

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
        File::options().read(true).write(true).open(path)
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
    fn truncate(&self, length: usize) -> io::Result<()>;

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

        log::debug!("Vectors to write: {}", buf.len());

        let iovecs = buf.chunks_mut(*MAX_VECTORED_IO_BUFFERS);

        for mut iovec in iovecs {
            log::debug!("Batch size: {}", iovec.len());
            while !iovec.is_empty() {
                let written = unsafe {
                    libc::pwritev(
                        self.as_raw_fd(),
                        iovec.as_ptr().cast(),
                        iovec.len() as libc::c_int,
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
                while !iovec.is_empty() && written >= iovec[0].len() {
                    written -= iovec[0].len();
                    iovec = &mut iovec[1..];
                }

                // advance iovec if part of it was written
                if !iovec.is_empty() && written > 0 {
                    iovec[0].advance(written);
                }
            }
            log::trace!("batch write completed");
        }

        Ok(total_written)
    }

    fn truncate(&self, length: usize) -> io::Result<()> {
        let fd = self.as_raw_fd();

        let res = unsafe { libc::ftruncate(fd, length as libc::off_t) };

        if res != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

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
    /// Unsafe wrapper for io operations. In order to make it safe, `I` must be
    ///  safe to operate in multi-threaded envirioment. Example: Linux `pread` and `pwrite`.
    io: UnsafeCell<I>,
    /// Size of single block in **bytes**.
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

    fn get_io(&self) -> &mut I {
        unsafe { self.io.get().as_mut().unwrap() }
    }

    /// Calculates offset to block (starts at 1). Includes header size.
    pub fn calculate_offset(&self, block_number: BlockNumber) -> io::Result<usize> {
        if block_number < 1 {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }

        Ok((block_number - 1) as usize * self.block_size)
    }
}

impl<I: IO> BlockIO<I> {
    pub fn raw_read(&self, offset: usize, buffer: &mut [u8]) -> io::Result<usize> {
        self.get_io().pread(offset, buffer)
    }

    /// Reads header from beginning of a file. Note that buffer size must match
    /// header size. This operation is atomic.
    pub fn read_header(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.raw_read(0, buffer)
    }

    /// Reads block with given number. Includes header offset. This operation is
    /// atomic. `block_number starts at 1`. Also `skip` param allows to skip certain
    /// ammount of bytes. Usefull when you want to read page from WAL skipping
    /// frame header.
    pub fn read(
        &self,
        block_number: BlockNumber,
        buffer: &mut Buffer,
        skip: usize,
    ) -> io::Result<usize> {
        let offset = self.calculate_offset(block_number)? + skip;

        self.raw_read(offset, buffer.as_mut_slice())
    }

    pub fn raw_write(&self, offset: usize, buffer: &[u8]) -> io::Result<usize> {
        self.get_io().pwrite(offset, buffer)
    }

    /// Writes header at the beginning of a file. Note that buffer size must
    /// match header size. This operation is atomic.
    pub fn write_header(&self, buffer: &[u8]) -> io::Result<usize> {
        self.raw_write(0, buffer)
    }

    /// Writes block at given number. Includes header offset. This operation is
    /// atomic. `page_number starts at 1`. Also `skip` param allows to skip certain
    /// ammount of bytes. Usefull when you want to write page to WAL skipping
    /// frame header.
    pub fn write(
        &self,
        block_number: BlockNumber,
        buffer: &Buffer,
        skip: usize,
    ) -> io::Result<usize> {
        let offset = self.calculate_offset(block_number)? + skip;

        self.raw_write(offset, buffer.as_slice())
    }

    /// Writes sequence of block starting at `block_number`. Note that buffers
    /// are structured like this: header + content. So to write single block
    /// you need to use two [`IoSlice`]`s`.
    pub fn write_vectored(
        &self,
        block_number: BlockNumber,
        buffers: &mut [IoSlice],
        skip: usize,
    ) -> io::Result<usize> {
        if buffers.len() % 2 != 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let offset = self.calculate_offset(block_number)? + skip;

        self.get_io().pwrite_vec(offset, buffers)
    }

    /// Truncates file to specific block length. If `to_block_len`
    /// is 0, then only header will be left.
    pub fn truncate(&self, to_block_len: BlockNumber) -> io::Result<()> {
        let len_in_bytes = self.header_size + (to_block_len as usize * self.block_size);

        self.get_io().truncate(len_in_bytes)
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
    /// File after: [Header, Block 1 (before 4), Block 2 (before 5)]
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

impl<I: Write + FileOps> BlockIO<I> {
    /// Flushes and fsync all writes to make sure that they are persisted.
    pub fn persist(&self) -> io::Result<()> {
        self.flush()?;
        self.sync()
    }
}
