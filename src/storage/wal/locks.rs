use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use parking_lot::{Condvar, Mutex};

use crate::utils::bytes::{pack_u64, unpack_u64};

/// Counting semaphore. Used to sync readers.
pub struct Semaphore {
    count: Mutex<usize>,
    state: Condvar,
}

impl Semaphore {
    pub fn new(initial: usize) -> Self {
        Self {
            count: Mutex::new(initial),
            state: Condvar::new(),
        }
    }

    /// Tryies to acquire permit, but if none is avalible it will block current thread.
    pub fn acquire(&self) {
        let mut count = self.count.lock();

        while *count == 0 {
            self.state.wait(&mut count);
        }

        *count -= 1;
    }

    /// Releases permit and notifies one thread.
    pub fn release(&self) {
        let mut count = self.count.lock();
        *count += 1;
        self.state.notify_one();
    }
}

pub struct ReadersPool {
    permits: Arc<Semaphore>,
}

impl ReadersPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub fn get_permit(&self) -> ReadGuard {
        self.permits.acquire();
        ReadGuard {
            semaphore: self.permits.clone(),
        }
    }
}

pub struct ReadGuard {
    semaphore: Arc<Semaphore>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.semaphore.release();
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
