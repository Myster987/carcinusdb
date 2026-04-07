use std::collections::HashSet;

use parking_lot::MutexGuard;

use crate::{
    storage::{
        self, FrameNumber, PageNumber,
        page::DatabaseHeader,
        wal::{READERS_NUM, WriteAheadLog},
    },
    utils::concurrency::SlotGuard,
};

pub struct Transaction {
    min_frame: FrameNumber,
    max_frame: FrameNumber,
    lock_level: LockLevel,
    dirty_pages: HashSet<PageNumber>,
}

impl Transaction {
    pub fn new(min_frame: FrameNumber, max_frame: FrameNumber) -> Self {
        Self {
            min_frame,
            max_frame,
            lock_level: LockLevel::None,
            dirty_pages: HashSet::new(),
        }
    }

    #[inline]
    pub fn max_frame(&self) -> FrameNumber {
        self.max_frame
    }

    #[inline]
    pub fn set_max_frame(&mut self, value: FrameNumber) {
        self.max_frame = value;
    }

    pub fn local_db_header(&self) -> &DatabaseHeader {
        match &self.lock_level {
            LockLevel::Write {
                local_db_header, ..
            } => local_db_header,
            LockLevel::Exclusive {
                local_db_header, ..
            } => local_db_header,
            _ => panic!("Called on wrong transaction type."),
        }
    }

    pub fn local_db_header_mut(&mut self) -> &mut DatabaseHeader {
        match &mut self.lock_level {
            LockLevel::Write {
                local_db_header, ..
            } => local_db_header,
            LockLevel::Exclusive {
                local_db_header, ..
            } => local_db_header,
            _ => panic!("Called on wrong transaction type."),
        }
    }

    #[inline]
    pub fn dirty_pages(&self) -> &HashSet<PageNumber> {
        &self.dirty_pages
    }

    #[inline]
    pub fn dirty_pages_mut(&mut self) -> &mut HashSet<PageNumber> {
        &mut self.dirty_pages
    }

    pub fn release_lock(&mut self) {
        self.lock_level = LockLevel::None;
    }

    pub fn acquire_read(&mut self, wal: &WriteAheadLog) -> storage::Result<()> {
        // log::trace!("Acquire read acess for transaction");
        if self.lock_level.is_at_least_read() {
            return Ok(());
        }

        let guard = wal.readers.acquire(self.min_frame);
        self.lock_level = LockLevel::Read(guard);

        Ok(())
    }

    pub fn acquire_write(&mut self, wal: &WriteAheadLog) -> storage::Result<()> {
        // log::trace!("Acquire write acess for transaction");
        if self.lock_level.is_at_least_write() {
            return Ok(());
        }

        // SAFETY: This needs to be handled carefully because this makes life
        // so much simpler with compipler, but also removes protection from
        // not dropping on time, but code logic should handle it properly.
        let guard = unsafe { std::mem::transmute(wal.writer.lock()) };
        let local_db_header = *wal.db_header.load_full();

        self.lock_level = LockLevel::Write {
            guard,
            local_db_header,
        };

        Ok(())
    }

    pub fn acquire_exclusive(&mut self, wal: &WriteAheadLog) -> storage::Result<()> {
        // log::trace!("Acquire exclusive acess for transaction");
        if self.lock_level.is_exclusive() {
            return Ok(());
        }

        // SAFETY: This needs to be handled carefully because this makes life
        // so much simpler with compipler, but also removes protection from
        // not dropping on time, but code logic should handle it properly.
        let guard = unsafe { std::mem::transmute(wal.writer.lock()) };
        let local_db_header = *wal.db_header.load_full();

        self.lock_level = LockLevel::Exclusive {
            guard,
            local_db_header,
        };

        Ok(())
    }
}

pub enum LockLevel {
    None,
    Read(SlotGuard<READERS_NUM>),
    Write {
        guard: MutexGuard<'static, ()>,
        local_db_header: DatabaseHeader,
    },
    /// Commit only.
    Exclusive {
        guard: MutexGuard<'static, ()>,
        local_db_header: DatabaseHeader,
    },
}

impl LockLevel {
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
