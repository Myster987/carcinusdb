use std::{cell::UnsafeCell, sync::atomic::AtomicUsize};

use parking_lot::{Mutex, MutexGuard};

pub struct HybridLock<T> {
    version: AtomicUsize,
    lock: Mutex<()>,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for HybridLock<T> {}
unsafe impl<T: Send> Sync for HybridLock<T> {}


impl<T> HybridLock<T> {
    pub fn new(val: T) -> HybridLock<T> {
        Self {
            version: AtomicUsize::new(0),
            lock: Mutex::new(()),
            data: UnsafeCell::new(val),
        }
    }
}
pub struct HybridLockGuard<'a, T> {
    lock: &'a HybridLock<T>,
    guard: MutexGuard<'a, ()>,
}

