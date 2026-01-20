use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
};

use crossbeam::utils::Backoff;
use parking_lot::{MutexGuard, RwLockReadGuard};

use crate::{
    storage::FrameNumber,
    utils::bytes::{pack_u64, unpack_u64},
};

/// Pool of all avalible readers that are allowed to read from WAL.
pub struct ReadersPool<const READERS_NUM: usize> {
    /// Array of atomic u32 that keeps track of `min_frame` for each reader.
    slots: [AtomicU32; READERS_NUM],
    /// [`Backoff`] to don't spam other atomic when trying to acquire read lock.
    backoff: Backoff,
}

impl<const READERS_NUM: usize> ReadersPool<READERS_NUM> {
    const FREE_SLOT: FrameNumber = 0;

    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU32::new(Self::FREE_SLOT)),
            backoff: Backoff::new(),
        }
    }

    /// Returns smallest active frame that is currently visible. If no lock is held,
    /// then it returns `FrameNumber::MAX`.
    pub fn min_active_frame(&self) -> FrameNumber {
        let mut min_frame = FrameNumber::MAX;

        for slot in &self.slots {
            let value = slot.load(Ordering::Acquire);
            if value != Self::FREE_SLOT {
                min_frame = std::cmp::min(min_frame, value);
            }
        }

        min_frame
    }

    /// Retruns `min_frame` at given slot. This operation is atomic.
    pub fn get_min_frame(&self, slot_id: usize) -> FrameNumber {
        self.slots[slot_id].load(Ordering::Acquire)
    }

    /// Acquires read lock and sets `min_visible` frame for it with `get_min_visible` fn.
    /// When all locks are in use it starts exponential backoff (if backoff reaches
    /// certein point, it updates `min_visible`).
    pub fn acquire<'a, F>(
        self: Arc<Self>,
        checkpoint_guard: RwLockReadGuard<'a, ()>,
        get_min_visible: F,
    ) -> ReadGuard<'a, READERS_NUM>
    where
        F: Fn() -> FrameNumber,
    {
        let mut min_visible_frame = get_min_visible();

        loop {
            for (i, slot) in self.slots.iter().enumerate() {
                if slot
                    .compare_exchange(
                        Self::FREE_SLOT,
                        min_visible_frame,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.backoff.reset();
                    return ReadGuard {
                        _checkpoint_guard: checkpoint_guard,
                        slot_id: i,
                        pool: self.clone(),
                    };
                }
            }
            self.backoff.spin();

            if self.backoff.is_completed() {
                min_visible_frame = get_min_visible();
            }
        }
    }

    pub fn release(&self, slot_id: usize) {
        self.slots[slot_id].store(0, Ordering::Release);
    }
}

/// Wrapper of two lock guard to hold them in single place.
pub struct ReadGuard<'a, const READERS_NUM: usize> {
    _checkpoint_guard: RwLockReadGuard<'a, ()>,
    pool: Arc<ReadersPool<READERS_NUM>>,
    slot_id: usize,
}

impl<'a, const READERS_NUM: usize> ReadGuard<'a, READERS_NUM> {
    pub fn min_frame(&self) -> FrameNumber {
        self.pool.get_min_frame(self.slot_id)
    }
}

impl<'a, const READERS_NUM: usize> Drop for ReadGuard<'a, READERS_NUM> {
    fn drop(&mut self) {
        self.pool.release(self.slot_id);
    }
}

/// Wrapper of two lock guard to hold them in single place.
pub struct WriteGuard<'a> {
    _checkpoint_guard: RwLockReadGuard<'a, ()>,
    _mutex_guard: MutexGuard<'a, ()>,
}

impl<'a> WriteGuard<'a> {
    pub fn new(checkpoint_guard: RwLockReadGuard<'a, ()>, mutex_guard: MutexGuard<'a, ()>) -> Self {
        Self {
            _checkpoint_guard: checkpoint_guard,
            _mutex_guard: mutex_guard,
        }
    }
}

/// Allows for storing two u32 in single u64. Makes read and write operations to
/// both numbers atomics, so we don't have to deal with making snapshots consistent.
pub trait PackedU64 {
    fn load_packed(&self, order: Ordering) -> (u32, u32);
    fn store_packed(&self, val_1: u32, val_2: u32, order: Ordering);
}

impl PackedU64 for AtomicU64 {
    fn load_packed(&self, order: Ordering) -> (u32, u32) {
        unpack_u64(self.load(order))
    }
    fn store_packed(&self, val_1: u32, val_2: u32, order: Ordering) {
        self.store(pack_u64(val_1, val_2), order);
    }
}
