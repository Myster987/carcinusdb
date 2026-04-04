use parking_lot::MutexGuard;

use crate::{
    storage::{
        self, FrameNumber,
        page::DatabaseHeader,
        wal::{READERS_NUM, WriteAheadLog},
    },
    utils::concurrency::SlotGuard,
};

pub struct Transaction<'a> {
    min_frame: FrameNumber,
    max_frame: FrameNumber,
    lock_level: LockLevel<'a>,
}

impl<'a> Transaction<'a> {
    pub fn new(min_frame: FrameNumber, max_frame: FrameNumber) -> Self {
        Self {
            min_frame,
            max_frame,
            lock_level: LockLevel::None,
        }
    }

    pub fn acquire_read(&mut self, wal: &WriteAheadLog) -> storage::Result<()> {
        if self.lock_level.is_at_least_read() {
            return Ok(());
        }

        let guard = wal.readers.acquire(self.min_frame);
        self.lock_level = LockLevel::Read(guard);

        Ok(())
    }

    pub fn acquire_write(&mut self, wal: &'a WriteAheadLog) -> storage::Result<()> {
        if self.lock_level.is_at_least_write() {
            return Ok(());
        }

        let guard = wal.writer.lock();
        let local_db_header = *wal.db_header.load_full();
        self.lock_level = LockLevel::Write {
            guard,
            local_db_header,
        };

        Ok(())
    }

    pub fn acquire_exclusive(&mut self, wal: &'a WriteAheadLog) -> storage::Result<()> {
        if self.lock_level.is_exclusive() {
            return Ok(());
        }

        let guard = wal.writer.lock();
        let local_db_header = *wal.db_header.load_full();
        self.lock_level = LockLevel::Exclusive {
            guard,
            local_db_header,
        };

        Ok(())
    }
}

pub enum LockLevel<'a> {
    None,
    Read(SlotGuard<READERS_NUM>),
    Write {
        guard: MutexGuard<'a, ()>,
        local_db_header: DatabaseHeader,
    },
    /// Commit only.
    Exclusive {
        guard: MutexGuard<'a, ()>,
        local_db_header: DatabaseHeader,
    },
}

impl<'a> LockLevel<'a> {
    fn rank(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Read(_) => 1,
            Self::Write { .. } => 2,
            Self::Exclusive { .. } => 3,
        }
    }

    pub fn is_at_least_read(&self) -> bool {
        self.rank() >= 1
    }

    pub fn is_at_least_write(&self) -> bool {
        self.rank() >= 2
    }

    pub fn is_exclusive(&self) -> bool {
        self.rank() >= 3
    }
}

/// Generic trait for both read and write transactions.
pub trait ReadTx {
    /// Returns max_frame stored by transaction locally.
    fn tx_max_frame(&self) -> FrameNumber;
}

/// Trait for write transaction. Write transaction must allow both read and write.
pub trait WriteTx: ReadTx {
    /// Sets new local max_frame for transaction.
    fn tx_set_max_frame(&mut self, new_max_frame: FrameNumber);
    // /// Sets new local last_checksum for transaction.
    // fn tx_set_last_checksum(&mut self, new_checksum: Checksum);
    fn tx_local_db_header(&self) -> &DatabaseHeader;
    fn tx_local_db_header_mut(&mut self) -> &mut DatabaseHeader;
}

/// Read transaction that holds all the necessary guards and local variables.
pub struct ReadTransaction {
    /// There can by only limited number of transactions running at the same
    /// time, so we have to acquire lock to even do something.
    read_guard: SlotGuard<READERS_NUM>,

    /// Minimal frame visible to this transaction in WAL.
    min_frame: FrameNumber,
    /// Maximal frame visible to this transaction in WAL.
    max_frame: FrameNumber,
}

impl ReadTransaction {
    /// Starts new read transaction. Migth need to re-run, because writer
    /// changed some metadata.
    pub fn begin(wal: &WriteAheadLog) -> storage::Result<Self> {
        let min_frame = wal.get_min_frame();
        let max_frame = wal.get_max_frame();

        let read_guard = wal.readers.acquire(min_frame);

        // if durring acquiring reade guard frames change, we need to try again.
        if min_frame != wal.get_min_frame() || max_frame != wal.get_max_frame() {
            return Err(storage::Error::RetryTransaction);
        }

        Ok(Self {
            read_guard,
            min_frame,
            max_frame,
        })
    }
}

impl ReadTx for ReadTransaction {
    #[inline]
    fn tx_max_frame(&self) -> FrameNumber {
        self.max_frame
    }
}

/// Write transaction - must be ended with wal.commit().
pub struct WriteTransaction<'a> {
    pub write_guard: MutexGuard<'a, ()>,
    pub max_frame: FrameNumber,
    pub local_db_header: DatabaseHeader,
}

impl<'a> WriteTransaction<'a> {
    pub fn begin(wal: &'a WriteAheadLog) -> storage::Result<Self> {
        // let checkpoint_guard = wal.checkpoint_lock.read();
        let write_guard = wal.writer.lock();

        let max_frame = wal.get_max_frame();
        let local_db_header = *wal.db_header.load_full();

        // ignore for now because locks migth be enough
        // // check if this changed and if so, we need to retry.
        // // I don't know how this would affect locks but maybe
        // // simple while loop would be enough
        // if min_frame != wal.get_min_frame()
        //     || max_frame != wal.get_max_frame()
        //     || last_checksum != wal.get_last_checksum()
        // {
        //     return Err(storage::Error::RetryTransaction);
        // }

        Ok(Self {
            write_guard,
            max_frame,
            local_db_header,
        })
    }
}

impl ReadTx for WriteTransaction<'_> {
    #[inline]
    fn tx_max_frame(&self) -> FrameNumber {
        self.max_frame
    }
}

impl WriteTx for WriteTransaction<'_> {
    #[inline]
    fn tx_set_max_frame(&mut self, new_max_frame: FrameNumber) {
        self.max_frame = new_max_frame;
    }

    #[inline]
    fn tx_local_db_header(&self) -> &DatabaseHeader {
        &self.local_db_header
    }

    #[inline]
    fn tx_local_db_header_mut(&mut self) -> &mut DatabaseHeader {
        &mut self.local_db_header
    }
}
