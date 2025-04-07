use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::storage::PageNumber;

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

#[derive(Debug)]
pub struct BlockIO<I> {
    io: I,
    pub block_size: usize,
    pub page_size: usize,
}

impl<I> BlockIO<I> {
    pub fn new(io: I, block_size: usize, page_size: usize) -> Self {
        Self {
            io,
            block_size,
            page_size,
        }
    }
}

impl<I: Seek + Read> BlockIO<I> {
    pub fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> io::Result<usize> {
        let offset;
        let capacity;
        let inner_offset;

        let Self {
            block_size,
            page_size,
            ..
        } = *self;
        let page_number = page_number as usize;

        if page_size >= block_size {
            capacity = page_size;
            offset = page_size * page_number;
            inner_offset = 0;
        } else {
            capacity = block_size;
            offset = (page_size * page_number) & !(block_size - 1);
            inner_offset = page_number * page_size - offset;
        }

        self.io.seek(SeekFrom::Start(offset as u64))?;

        if page_size >= block_size {
            return self.io.read(buffer);
        }

        let mut block = vec![0; capacity];
        let _ = self.io.read(&mut block);

        buffer.copy_from_slice(&block[inner_offset..inner_offset + page_size]);

        Ok(self.page_size)
    }
}

impl<I: Seek + Write> BlockIO<I> {
    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> io::Result<usize> {
        let offset = page_number * self.page_size as u32;

        self.io.seek(SeekFrom::Start(offset as u64))?;

        self.io.write(buffer)
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
