use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crossbeam::utils::{Backoff, CachePadded};
use parking_lot::{Condvar, Mutex};

use crate::{
    storage::FrameNumber,
    utils::bytes::{pack_u64, unpack_u64},
};

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

pub struct AtomicArray<const SIZE: usize> {
    slots: [CachePadded<AtomicU64>; SIZE],
    backoff: Backoff,
}

impl<const SIZE: usize> AtomicArray<SIZE> {
    const FREE_SLOT: u32 = 0;

    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| CachePadded::new(AtomicU64::new(0))),
            backoff: Backoff::new(),
        }
    }

    fn spin(&self) {
        self.backoff.spin();

        if self.backoff.is_completed() {
            self.backoff.snooze();
        }
    }

    pub fn acquire(self: &Arc<Self>, min_visible: FrameNumber) -> SlotGuard<SIZE> {
        loop {
            for (i, slot) in self.slots.iter().enumerate() {
                let current = slot.load(Ordering::Acquire);
                let (current_value, current_tag) = unpack_u64(current);

                if current_value != Self::FREE_SLOT {
                    self.spin();
                    continue;
                }

                let new = pack_u64(min_visible, current_tag.wrapping_add(1));

                // try to claim this slot
                if slot
                    .compare_exchange(current, new, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.backoff.reset();

                    return SlotGuard {
                        array: self.clone(),
                        slot_number: i,
                    };
                }
            }

            self.spin();
        }
    }

    fn release(&self, index: usize) {
        let (_, tag) = self.slots[index].load_packed(Ordering::Acquire);
        self.slots[index].store_packed(Self::FREE_SLOT, tag.wrapping_add(1), Ordering::Release);
    }

    pub fn min_visible_frame(&self) -> Option<FrameNumber> {
        loop {
            let before: Vec<(u32, u32)> = self
                .slots
                .iter()
                .map(|s| s.load_packed(Ordering::Acquire))
                .collect();

            let after: Vec<(u32, u32)> = self
                .slots
                .iter()
                .map(|s| s.load_packed(Ordering::Acquire))
                .collect();

            if before == after {
                self.backoff.reset();

                let min_frame = after
                    .iter()
                    .filter_map(|&(val, _)| {
                        if val != Self::FREE_SLOT {
                            Some(val)
                        } else {
                            None
                        }
                    })
                    .min();

                return min_frame;
            }

            self.spin();
        }
    }
}

pub struct SlotGuard<const SIZE: usize> {
    array: Arc<AtomicArray<SIZE>>,
    slot_number: usize,
}

impl<const SIZE: usize> Drop for SlotGuard<SIZE> {
    fn drop(&mut self) {
        self.array.release(self.slot_number);
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
