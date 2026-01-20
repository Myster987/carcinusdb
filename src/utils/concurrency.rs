use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

/// Counting semaphore. Used to sync readers.
pub struct Semaphore {
    count: Mutex<usize>,
    state: Condvar,
}

impl Semaphore {
    pub fn new(capacity: usize) -> Self {
        Self {
            count: Mutex::new(capacity),
            state: Condvar::new(),
        }
    }

    /// Tryies to acquire permit, but if none is avalible it will block current thread.
    pub fn acquire(self: &Arc<Self>) -> SemaphoreGuard {
        let mut count = self.count.lock();

        while *count == 0 {
            self.state.wait(&mut count);
        }

        *count -= 1;

        SemaphoreGuard {
            semaphore: self.clone(),
        }
    }

    /// Releases permit and notifies one thread.
    pub fn release(&self) {
        let mut count = self.count.lock();
        *count += 1;
        self.state.notify_one();
    }
}

pub struct SemaphoreGuard {
    semaphore: Arc<Semaphore>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}
