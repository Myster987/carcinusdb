use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::Arc,
};

use hashbrown::{DefaultHashBuilder, HashMap};
use parking_lot::Mutex;
use thiserror::Error;

use crate::storage::{PageNumber, pager::MemPage};

const AGGRESSIVE_THRESHOLD: usize = 2;
const MAX_USAGE_COUNT: u8 = 5;
const DEFAULT_SHARD_COUNT: usize = 8;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("all entires in cache are pinned. can't evict any entry now.")]
    AllPagesPinned,
}

pub struct ShardedClockCache<S: BuildHasher = DefaultHashBuilder> {
    shards: Vec<Mutex<ClockCache>>,
    hasher: S,
}

unsafe impl Send for ShardedClockCache {}
unsafe impl Sync for ShardedClockCache {}

impl ShardedClockCache {
    pub fn new(capacity: usize) -> Self {
        let per_shard_capacity = (capacity + DEFAULT_SHARD_COUNT - 1) / DEFAULT_SHARD_COUNT;

        Self {
            shards: (0..DEFAULT_SHARD_COUNT)
                .map(|_| Mutex::new(ClockCache::new(per_shard_capacity)))
                .collect(),
            hasher: DefaultHashBuilder::default(),
        }
    }
}

impl<S: BuildHasher> ShardedClockCache<S> {
    fn get_shard(&self, key: &PageNumber) -> &Mutex<ClockCache> {
        let mut hasher = self.hasher.build_hasher();

        key.hash(&mut hasher);

        // key.hash(&mut hasher);

        let h = hasher.finish() as usize;
        let shard_idx = h % DEFAULT_SHARD_COUNT;

        &self.shards[shard_idx]
    }

    pub fn get(&self, key: &PageNumber) -> Option<Arc<MemPage>> {
        self.get_shard(key).lock().get(key)
    }

    pub fn insert(&self, key: PageNumber, page: Arc<MemPage>) -> Result<()> {
        self.get_shard(&key).lock().try_insert(key, page)
    }
}

struct ClockEntry {
    page: Arc<MemPage>,
    usage_count: u8,
}

impl ClockEntry {
    fn new(page: Arc<MemPage>) -> Self {
        Self {
            page,
            usage_count: 0,
        }
    }
}

pub struct ClockCache {
    capacity: usize,
    pages: Vec<Option<ClockEntry>>,
    /// key: page number, value: index in clock.
    map: HashMap<PageNumber, usize>,
    /// Current clock hand position.
    hand: usize,
}

impl ClockCache {
    pub fn new(capacity: usize) -> Self {
        // initialize empty
        let mut pages = Vec::with_capacity(capacity);
        pages.resize_with(capacity, || None);

        Self {
            capacity,
            pages,
            map: HashMap::with_capacity(capacity),
            hand: 0,
        }
    }

    pub fn try_insert(&mut self, key: PageNumber, page: Arc<MemPage>) -> Result<()> {
        // update in place
        if let Some(index) = self.map.get(&key) {
            self.pages[*index] = Some(ClockEntry::new(page));
            return Ok(());
        }
        // free spot
        let insert_at = self.make_room()?;

        // replace old entry
        let old_one = self.pages[insert_at].replace(ClockEntry::new(page));

        // delete old key
        if let Some(old_entry) = old_one {
            self.map.remove(&old_entry.page.id());
        }

        // insert new key
        self.map.insert(key, insert_at);

        Ok(())
    }

    pub fn get(&mut self, key: &PageNumber) -> Option<Arc<MemPage>> {
        if let Some(index) = self.map.get(key) {
            let entry = self.pages[*index].as_mut().unwrap();

            if entry.usage_count < MAX_USAGE_COUNT {
                entry.usage_count += 1;
            }

            Some(entry.page.clone())
        } else {
            None
        }
    }

    /// Searches for free entry in cache or one that can be evicted and returns it's postion.
    fn make_room(&mut self) -> Result<usize> {
        let start_postion = self.hand;
        let mut current_position = self.hand;
        let mut rotation_count = 0;
        let mut pinned_count = 0;

        loop {
            if let Some(entry) = &mut self.pages[current_position] {
                if entry.page.pin_count() == 0 || !entry.page.is_dirty() {
                    if entry.usage_count == 0 {
                        self.hand = (current_position + 1) % self.capacity;
                        return Ok(current_position);
                    }
                    // if we make 2 or more full rotations then we need to
                    // start substracting more
                    entry.usage_count = entry.usage_count.saturating_sub(
                        if rotation_count >= AGGRESSIVE_THRESHOLD {
                            2
                        } else {
                            1
                        },
                    );
                } else {
                    pinned_count += 1;
                }
            } else {
                // empty entry
                self.hand = (current_position + 1) % self.capacity;
                return Ok(current_position);
            }

            current_position = (current_position + 1) % self.capacity;

            if pinned_count >= self.capacity && current_position == start_postion {
                break;
            }

            if current_position == start_postion {
                rotation_count += 1;
            }
        }

        Err(Error::AllPagesPinned)
    }
}
