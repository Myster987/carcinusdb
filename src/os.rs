use std::{
    fs::{self, File},
    io,
    path::Path,
    sync::LazyLock,
};

/// Contains disk block size.
/// # Note
/// It uses `/etc/passwd` file that is allways present on Linux/Unix but not on Windows (so it won't work)
pub static DISK_BLOCK_SIZE: LazyLock<usize> = LazyLock::new(|| {
    Fs::block_size("/etc/passwd").expect("Something when wrong when calculating disk block size.")
});

pub trait FileSystemBlockSize {
    fn block_size(path: impl AsRef<Path>) -> io::Result<usize>;
}

pub trait Open {
    fn open(self, path: impl AsRef<Path>) -> io::Result<File>;
}

pub struct Fs;

pub struct OpenOptions {
    inner: fs::OpenOptions,
    bypass_cache: bool,
    sync_on_write: bool,
    lock: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            inner: File::options(),
            bypass_cache: false,
            sync_on_write: false,
            lock: false,
        }
    }
}

impl OpenOptions {
    /// disable OS
    pub fn bypass_cache(mut self, bypass_cache: bool) -> Self {
        self.bypass_cache = bypass_cache;
        self
    }

    /// each .write() will make sure that data is writen to the disk.
    pub fn sync_on_write(mut self, sync_on_write: bool) -> Self {
        self.sync_on_write = sync_on_write;
        self
    }

    /// Create if doesn't exists
    pub fn create(mut self, create: bool) -> Self {
        self.inner.create(create);
        self
    }

    /// Open for reading
    pub fn read(mut self, read: bool) -> Self {
        self.inner.read(read);
        self
    }

    /// Open for writing
    pub fn write(mut self, write: bool) -> Self {
        self.inner.write(write);
        self
    }

    /// Locks the file with exclusive access for the calling process.
    ///
    /// File won't actually be locked until [`Self::open`] is called.
    pub fn lock(mut self, lock: bool) -> Self {
        self.lock = lock;
        self
    }

    /// Set length of file to 0 bytes if exists
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.inner.truncate(truncate);
        self
    }
}

#[cfg(unix)]
mod unix {

    use std::{
        fs::File,
        io,
        os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
        path::Path,
    };

    use super::{FileSystemBlockSize, Fs, Open, OpenOptions};

    impl FileSystemBlockSize for Fs {
        fn block_size(path: impl AsRef<std::path::Path>) -> io::Result<usize> {
            Ok(File::open(&path)?.metadata()?.blksize() as usize)
        }
    }

    impl Open for OpenOptions {
        fn open(mut self, path: impl AsRef<Path>) -> io::Result<File> {
            let mut flags = 0;

            if self.bypass_cache {
                flags |= libc::O_DIRECT;
            }

            if self.sync_on_write {
                flags |= libc::O_DSYNC;
            }

            if flags != 0 {
                self.inner.custom_flags(flags);
            }

            let file = self.inner.open(&path)?;

            if self.lock {
                let lock = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };

                if lock != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("could not lock file {}", path.as_ref().display()),
                    ));
                }
            }

            Ok(file)
        }
    }
}
