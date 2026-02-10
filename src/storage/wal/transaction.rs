use std::mem::ManuallyDrop;

use parking_lot::MutexGuard;

use crate::{
    storage::{
        self, FrameNumber,
        wal::{READERS_NUM, WriteAheadLog},
    },
    utils::concurrency::SlotGuard,
};

/// Generic trait for both read and write transactions.
pub trait ReadTx {
    /// Returns min_frame stored by transaction locally.
    fn tx_min_frame(&self) -> FrameNumber;
    /// Returns max_frame stored by transaction locally.
    fn tx_max_frame(&self) -> FrameNumber;
}

/// Trait for write transaction. Write transaction must allow both read and write.
pub trait WriteTx: ReadTx {
    // /// Returns last_checksum stored by transaction locally.
    // fn tx_last_checksum(&self) -> Checksum;
    /// Sets new local max_frame for transaction.
    fn tx_set_max_frame(&mut self, new_max_frame: FrameNumber);
    // /// Sets new local last_checksum for transaction.
    // fn tx_set_last_checksum(&mut self, new_checksum: Checksum);
}

/// Read transaction that holds all the necessary guards and local variables.
pub struct ReadTransaction {
    // /// Protects this transaction, so that checkpoint allows it to finish.
    // checkpoint_guard: RwLockReadGuard<'a, ()>,
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
            // checkpoint_guard,
            read_guard,
            min_frame,
            max_frame,
        })
    }
}

impl ReadTx for ReadTransaction {
    #[inline]
    fn tx_min_frame(&self) -> FrameNumber {
        self.min_frame
    }

    #[inline]
    fn tx_max_frame(&self) -> FrameNumber {
        self.max_frame
    }
}

/// Write transaction - must be ended with wal.commit().
pub struct WriteTransaction<'a> {
    pub inner: ManuallyDrop<WriteTransactionInner<'a>>,
}

/// Write transaction inner data.
pub struct WriteTransactionInner<'a> {
    pub write_guard: MutexGuard<'a, ()>,
    pub min_frame: FrameNumber,
    pub max_frame: FrameNumber,
}

impl<'a> WriteTransaction<'a> {
    pub fn begin(wal: &'a WriteAheadLog) -> storage::Result<Self> {
        // let checkpoint_guard = wal.checkpoint_lock.read();
        let write_guard = wal.writer.lock();

        let min_frame = wal.get_min_frame();
        let max_frame = wal.get_max_frame();

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
            inner: ManuallyDrop::new(WriteTransactionInner {
                // checkpoint_guard,
                write_guard,
                min_frame,
                max_frame,
            }),
        })
    }
}

impl ReadTx for WriteTransaction<'_> {
    #[inline]
    fn tx_min_frame(&self) -> FrameNumber {
        self.inner.min_frame
    }

    #[inline]
    fn tx_max_frame(&self) -> FrameNumber {
        self.inner.max_frame
    }
}

impl WriteTx for WriteTransaction<'_> {
    #[inline]
    fn tx_set_max_frame(&mut self, new_max_frame: FrameNumber) {
        self.inner.max_frame = new_max_frame;
    }
}
