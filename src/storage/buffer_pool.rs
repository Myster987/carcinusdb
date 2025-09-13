use std::{
    alloc::{Layout, alloc_zeroed},
    ptr::NonNull,
    rc::Rc,
};

use parking_lot::Mutex;

use crate::utils::{
    buffer::{BUFFER_ALIGNMENT, Buffer, BufferData, dealloc_heap},
    bytes::flip_n_bits,
};

/// Holds free buffers as pointers to memory.
pub struct BufferPool {
    pool: Mutex<PoolInner>,
    size: usize,
    page_size: usize,
}

impl BufferPool {
    pub fn new(size: usize, page_size: usize) -> Self {
        Self {
            size,
            page_size,
            pool: Mutex::new(PoolInner::new(size, page_size)),
        }
    }

    /// Returns total number of allocated buffers.
    pub fn len(&self) -> usize {
        self.pool.lock().len()
    }

    /// Returns free buffer
    pub fn get(self: &std::sync::Arc<Self>) -> Buffer {
        let (id, ptr) = self.pool.lock().alloc_one();
        let pool = self.clone();
        let drop_fn = Rc::new(move |id| pool.put(id));
        Buffer::from_pool(id, self.page_size, ptr, drop_fn)
    }

    /// Adds new free buffer to pool.
    pub fn put(&self, buffer_id: usize) {
        self.pool.lock().dealloc_one(buffer_id);
    }
}

fn geometric_sequence_sum(a1: usize, n: usize, q: usize) -> usize {
    if n == 0 {
        return 0;
    }
    a1 * ((q.pow(n as u32) - 1) / (q - 1))
}

fn geometric_sequence_nth(a1: usize, n: usize, q: usize) -> usize {
    if n == 0 {
        return a1;
    }
    a1 * q.pow(n as u32 - 1)
}

/// Internal block is made of 64 `u64` elements. Each element is sum of free elements that is points to by offset.
/// Let's say that we have this array [10, 5, 0, ..., 0] and total height is 2 so there is one internal node and 64 leaf nodes.
/// We want to flip 4 bits so we search this array. Find the closes number that is >= 4 and get it offset. In this case it will be 5 with offset 1.
/// Then we calculate total calculate offset to leaf node:
///
/// 1 internal block (8 * u64) + 1 leaf block (8 * u64) = 16 u64 to skip = 16 * 8 bytes
///
/// Offset will be calculate from geometric sum and then we will add leaf block * offset to skip.
///
/// Leaf block is array made of 8 `u64` elements. Total block bit capacity is 512
/// ```text
/// +--------------------------------------------------+
/// |                      leaf block                  |     
/// | 01010101 01010101 01010101 01010101 ... 01010101 |
/// +--------------------------------------------------+
/// ```
/// 1 - is free
/// 0 - is allocated
/// and in blocks u64 number is how many allocated are bellow
#[derive(Debug)]
struct BitMap {
    /// size in **bits** that can be flipped
    size: usize,
    /// tree height
    height: usize,
    /// pointer to beginning of bitmap memory
    ptr: NonNull<u64>,
}

impl BitMap {
    const WORD_SIZE: usize = size_of::<u64>();
    const WORD_SIZE_IN_BITS: usize = size_of::<u64>() * 8;
    const WORD_FULL: u64 = u64::MAX;

    const BLOCK_SIZE: usize = 8;
    const BLOCK_SIZE_IN_BYTES: usize = Self::BLOCK_SIZE * Self::WORD_SIZE;
    const BLOCK_SIZE_IN_BITS: usize = Self::BLOCK_SIZE * Self::WORD_SIZE_IN_BITS;

    pub fn new(size: usize) -> Self {
        assert!(
            size % Self::BLOCK_SIZE_IN_BITS == 0,
            "Size of {} must be divisible by {}",
            std::any::type_name::<Self>(),
            Self::BLOCK_SIZE_IN_BITS
        );

        let height = Self::find_tree_height(size);

        // each node contains 64 u64 ints so we need to increase this size as well
        let blocks_size = Self::sum_internal_nodes(height) * Self::BLOCK_SIZE_IN_BYTES;

        let layout =
            Layout::from_size_align(size / Self::WORD_SIZE + blocks_size, Self::WORD_SIZE).unwrap();
        let mem_ptr = unsafe { NonNull::new_unchecked(alloc_zeroed(layout)).cast() };

        Self {
            size,
            height,
            ptr: mem_ptr,
        }
    }

    /// Returns `Bitmap` size in **bits**.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns `Bitmap` size in **bytes**.
    pub fn size_in_bytes(&self) -> usize {
        self.size / Self::WORD_SIZE
    }

    /// Returns `Bitmap` index tree height.
    pub fn height(&self) -> usize {
        self.height
    }

    pub fn len(&self) -> usize {
        self.get_root_block().iter().map(|&v| v as usize).sum()
    }

    /// Finds tree height so all bits are covered.
    fn find_tree_height(size: usize) -> usize {
        let mut n = 1;

        // normal formula is n-1 but we want to find next one so we keep only n
        while geometric_sequence_nth(Self::BLOCK_SIZE_IN_BITS, n + 1, Self::WORD_SIZE) < size {
            n += 1;
        }

        n as usize
    }

    /// Returns how many blocks are from beginning to this point, depending
    /// on tree height. For now it will wast some space but later we will optimise it.
    /// This function uses formula for geometric sum: S = 1 * (1 - q^n) / (1 - q),
    /// but it's inversed so it won't underflow u64
    fn sum_internal_nodes(height: usize) -> usize {
        geometric_sequence_sum(1, height, Self::BLOCK_SIZE)
    }

    /// Returns how many **blocks** are on specific level.
    fn level_nodes_count(height: usize) -> usize {
        geometric_sequence_nth(1, height, Self::BLOCK_SIZE)
    }

    /// Returns offset in bytes to block based on it's level and block number.
    fn calculate_block_offset(&self, level: usize, block_number: usize) -> usize {
        let offset = self.size_in_bytes();
        let block_offset = block_number * Self::BLOCK_SIZE_IN_BYTES;
        let sum_of_prev = Self::sum_internal_nodes(level - 1) * Self::BLOCK_SIZE_IN_BYTES;

        offset + sum_of_prev + block_offset
    }

    /// Returns total size of bitmap including tree blocks in **bytes**
    pub fn total_size(&self) -> usize {
        let blocks_size = Self::sum_internal_nodes(self.height) * Self::BLOCK_SIZE_IN_BYTES;

        self.size + blocks_size
    }

    /// Returns word at given **byte** offset.
    fn get_word(&self, offset: usize) -> u64 {
        unsafe { self.ptr.byte_add(offset).read() }
    }

    /// Sets word value at given **byte** offset.
    fn set_word(&mut self, offset: usize, value: u64) {
        unsafe {
            self.ptr.byte_add(offset).write(value);
        }
    }

    /// Takes **byte** offset to block and number of bits to flip.
    /// Returns ids of bits that where flipped (id is **bit** number relative to beginning of `Bitmap`).
    fn flip_block_bits(&mut self, offset: usize, mut to_flip: usize) -> Vec<usize> {
        let block = self.get_block_mut_at_offset(offset);

        let mut ids = Vec::with_capacity(to_flip);

        for (i, word) in block.iter_mut().enumerate() {
            let free = word.leading_zeros() as usize;
            if free > 0 {
                let flip = if free > to_flip { to_flip } else { free };

                flip_n_bits(
                    word,
                    flip,
                    &mut ids,
                    (offset + i * Self::WORD_SIZE) * Self::WORD_SIZE,
                );
                // self.flip_word_bits(offset + i * Self::WORD_SIZE, flip);

                to_flip -= flip;
                if to_flip == 0 {
                    break;
                }
            }
        }

        ids
    }

    /// Returns pointer to beginning of a block at given **byte** offset.
    fn get_raw_block_at_offset(&self, offset: usize) -> NonNull<u64> {
        unsafe { self.ptr.byte_add(offset) }
    }

    /// Returns slice to block at given **byte** offset.
    fn get_block_at_offset(&self, offset: usize) -> &[u64] {
        unsafe {
            std::slice::from_raw_parts(
                self.get_raw_block_at_offset(offset).as_ptr().cast_const(),
                Self::BLOCK_SIZE,
            )
        }
    }

    /// Returns mutable slice to block at given **byte** offset.
    fn get_block_mut_at_offset(&mut self, offset: usize) -> &mut [u64] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_raw_block_at_offset(offset).as_ptr(),
                Self::BLOCK_SIZE,
            )
        }
    }

    /// Returns pointer to beginning of a block at given level.
    fn get_block_at_level(&self, level: usize, block_number: usize) -> NonNull<u64> {
        assert!(
            block_number < Self::level_nodes_count(level),
            "Attempted to access block {} that is outside of level nodes count.",
            block_number
        );

        let offset = self.calculate_block_offset(level, block_number);

        unsafe { self.ptr.byte_add(offset) }
    }

    /// Returns slice to block at given tree level and block number at this level.
    fn get_block(&self, level: usize, block_number: usize) -> &[u64] {
        unsafe {
            std::slice::from_raw_parts(
                self.get_block_at_level(level, block_number)
                    .as_ptr()
                    .cast_const(),
                Self::BLOCK_SIZE,
            )
        }
    }

    /// Returns mutable slice to block at given tree level and block number at this level.
    fn get_block_mut(&mut self, level: usize, block_number: usize) -> &mut [u64] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_block_at_level(level, block_number).as_ptr(),
                Self::BLOCK_SIZE,
            )
        }
    }

    /// Returns slice to root block.
    fn get_root_block(&self) -> &[u64] {
        self.get_block(1, 0)
    }

    /// Flips one bit and returns it's id.
    pub fn set_one(&mut self) -> Option<usize> {
        self.set_many(1).map(|mut vec| vec.pop().unwrap())
    }

    /// Flips given number of bits and returns their ids.
    pub fn set_many(&mut self, to_flip: usize) -> Option<Vec<usize>> {
        assert!(
            to_flip <= Self::WORD_SIZE_IN_BITS,
            "Can't flip more than {} at once",
            Self::WORD_SIZE_IN_BITS
        );
        // this stack will be later used to remember words that need to be updated when walking up
        let mut path_stack = vec![];

        let mut height = self.height;
        let mut block_level = 2;
        let mut block_offset = self.size_in_bytes();
        let mut block = self.get_root_block();

        while height > 0 {
            let free_index = Self::find_next_free_block_index(height, block, to_flip)?;

            path_stack.push((free_index, block_offset));
            block = self.get_block(block_level, free_index);
            block_offset = self.calculate_block_offset(block_level, free_index);

            height -= 1;
            block_level += 1;
        }

        let (leaf_tree_block_number, _) = path_stack.last().unwrap();
        let block_offset = *leaf_tree_block_number * Self::BLOCK_SIZE_IN_BYTES;

        self.backtrace_update(path_stack, to_flip as isize);

        Some(self.flip_block_bits(block_offset, to_flip))
    }

    /// Returns block number with at least `to_flip` bits free on next tree level.
    fn find_next_free_block_index(level: usize, block: &[u64], to_flip: usize) -> Option<usize> {
        // calculate max value of allocated bits on specific tree level, because when search
        let max_in_level = if level > 0 {
            Self::level_nodes_count(level) * Self::BLOCK_SIZE_IN_BITS
        } else {
            Self::WORD_SIZE_IN_BITS
        };
        let mut max = None;
        let mut index = None;
        for (i, &current) in block.iter().enumerate() {
            let current = current as usize;
            if max.is_none() && current.saturating_add(to_flip) <= max_in_level {
                max = Some(current);
                index = Some(i);
            }
            if max.is_some()
                && current > max.unwrap()
                && current.saturating_add(to_flip) <= max_in_level
            {
                max = Some(current);
                index = Some(i);
                if max.unwrap() == max_in_level {
                    break;
                }
            }
        }

        index
    }

    /// Backtraces update on tree using stack that contains index and byte offset to internal blocks to update.
    fn backtrace_update(&mut self, mut stack: Vec<(usize, usize)>, value: isize) {
        while let Some((index, block_offset_in_bytes)) = stack.pop() {
            let block = self.get_block_mut_at_offset(block_offset_in_bytes);
            if value >= 0 {
                block[index] += value as u64;
            } else {
                block[index] -= value.abs() as u64;
            }
        }
    }

    /// Takes beginning of leaf block in bits and then creates index update path.
    fn create_backtrace_path(&self, beginning: usize) -> Vec<(usize, usize)> {
        let mut height = self.height;
        let mut path_stack = vec![];

        let mut block_idx = beginning / Self::BLOCK_SIZE_IN_BITS;

        while height > 0 {
            let index = block_idx % Self::BLOCK_SIZE;
            let block_number = block_idx / Self::BLOCK_SIZE;

            let offset = self.calculate_block_offset(height, block_number);

            path_stack.push((index, offset));
            block_idx = block_number;
            height -= 1;
        }

        path_stack
    }

    /// Takes id and flips it's value to 0 and updates index.
    pub fn reset_one(&mut self, id: usize) {
        self.reset_many(&[id]);
    }

    /// Takes slice of ids (bit offsets) and flips all of them to 0 and updates index.
    /// # Safety
    /// In order for this function to work correctly ids needs to be from the **same block**, \
    /// if they are many given. Otherwise this function will panic when trying to reset bits out of range
    pub fn reset_many(&mut self, ids: &[usize]) {
        assert!(ids.len() > 0, "You must provide ids to reset.");

        // rounds up to multiple of BLOCK_SIZE_IN_BITS to find in which block ids are located,
        // because by design they must be in the same block to remove them
        let leaf_offset_in_bits =
            *ids.first().unwrap() / Self::BLOCK_SIZE_IN_BITS * Self::BLOCK_SIZE_IN_BITS;

        let to_flip = -(ids.len() as isize);

        let path_stack = self.create_backtrace_path(leaf_offset_in_bits);
        let leaf_block = self.get_block_mut_at_offset(leaf_offset_in_bits / Self::WORD_SIZE);

        for offset in ids.iter().map(|&id| id - leaf_offset_in_bits) {
            let index = offset / Self::WORD_SIZE_IN_BITS;
            let bit_number = offset % Self::WORD_SIZE_IN_BITS;

            leaf_block[index] ^= 1 << bit_number;
        }

        self.backtrace_update(path_stack, to_flip);
    }

    /// Used in debugging.
    #[cfg(debug_assertions)]
    fn get_mem(&self) -> &[u64] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().cast_const(), self.total_size()) }
    }

    /// Used in debugging.
    #[cfg(debug_assertions)]
    fn print_tree(&self) {
        for i in 1..=self.height {
            for block_number in 0..Self::level_nodes_count(i) {
                let offset = self.calculate_block_offset(i, block_number);

                let block = unsafe {
                    std::slice::from_raw_parts(
                        self.ptr.byte_add(offset).as_ptr().cast_const(),
                        Self::BLOCK_SIZE,
                    )
                };
                println!("#: {i}, offset (in u8) {offset}, block: {block:?}");
            }
        }
    }

    /// Used in debugging.
    #[cfg(debug_assertions)]
    fn print_leafs(&self) {
        let size = self.size / Self::BLOCK_SIZE_IN_BITS;
        for i in 0..size {
            let offset = i * Self::BLOCK_SIZE_IN_BYTES;
            let block = unsafe {
                std::slice::from_raw_parts(
                    self.ptr.byte_add(offset).as_ptr().cast_const(),
                    Self::BLOCK_SIZE,
                )
            };

            println!("leaf at: {offset} (in bytes), mem: {:?}", block);
        }
    }
}

impl Drop for BitMap {
    fn drop(&mut self) {
        let blocks_size = Self::sum_internal_nodes(self.height) * Self::BLOCK_SIZE_IN_BYTES;
        unsafe {
            dealloc_heap(
                self.ptr.cast(),
                self.size_in_bytes() + blocks_size,
                Self::WORD_SIZE,
            )
        };
    }
}

/// Fixed size memory pool that allocates big chunk of memory at once to speed up allocation.  
pub struct PoolInner {
    /// Size in memory blocks
    size: usize,
    /// Size of invidual block
    block_size: usize,
    /// Pointer to beginning of pool memory
    mem_ptr: NonNull<u8>,
    /// Bitmap index to speed up free bits lookup
    bitmap_index: BitMap,
}

impl PoolInner {
    // Creates new pool instance. Keep in mind that size is in blocks. So if `block_size` is 4KB and `size` is 100 it will allocate 400KB.
    pub fn new(size: usize, block_size: usize) -> Self {
        let layout = Layout::from_size_align(size * block_size, BUFFER_ALIGNMENT).unwrap();
        let mem_ptr = unsafe { NonNull::new_unchecked(alloc_zeroed(layout)) };

        Self {
            size,
            block_size,
            mem_ptr,
            bitmap_index: BitMap::new(size),
        }
    }

    /// Returns total allocated blocks.
    pub fn len(&self) -> usize {
        self.bitmap_index.len()
    }

    /// Returns pointer to memory at given **byte** offset.
    fn get_block(&mut self, offset: usize) -> BufferData {
        unsafe { self.mem_ptr.byte_add(offset) }
    }

    /// Allocates single blocks and returns id and pointer to memory.
    pub fn alloc_one(&mut self) -> (usize, BufferData) {
        self.alloc_many(1).pop().unwrap()
    }

    /// Allocates many blocks and returns vector of ids with memory pointers.
    pub fn alloc_many(&mut self, to_alloc: usize) -> Vec<(usize, BufferData)> {
        let ids = self.bitmap_index.set_many(to_alloc).expect("Pool is full");

        ids.into_iter()
            .map(|id| (id, self.get_block(id * self.block_size)))
            .collect()
    }

    /// Deallocates single block with given id. Keep in mind that it also cleans up memory.
    pub fn dealloc_one(&mut self, id: usize) {
        self.dealloc_many(&[id]);
    }

    /// Deallocates many block with given ids. Keep in mind that it also cleans up memory.
    pub fn dealloc_many(&mut self, ids: &[usize]) {
        for &id in ids {
            let offset = id * self.block_size;
            unsafe {
                self.mem_ptr
                    .byte_add(offset)
                    .write_bytes(0, self.block_size)
            };
        }
        self.bitmap_index.reset_many(ids);
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        unsafe { dealloc_heap(self.mem_ptr, self.size, BUFFER_ALIGNMENT) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool() -> anyhow::Result<()> {
        let mut pool = PoolInner::new(512, 16);

        println!("buf: {:?}", pool.alloc_one());
        println!("buf: {:?}", pool.alloc_one());

        Ok(())
    }

    #[test]
    fn test_bitmap() -> anyhow::Result<()> {
        // let mut bitmap = BitMap::new(512_000 + 512);

        // println!("{:?}", bitmap);

        // println!("level sum: {}, offset: {}", BitMap::level_nodes_count(1), (bitmap.calculate_block_offset(1, 1) - bitmap.size_in_bytes() - BitMap::level_nodes_count(0) * BitMap::BLOCK_SIZE_IN_BYTES) / BitMap::BLOCK_SIZE_IN_BYTES);
        // println!("level sum: {}, offset: {}", BitMap::level_nodes_count(2), (bitmap.calculate_block_offset(2, 15) - bitmap.size_in_bytes() - BitMap::level_nodes_count(1) * BitMap::BLOCK_SIZE_IN_BYTES) / BitMap::BLOCK_SIZE_IN_BYTES);
        // println!("level sum: {}, offset: {}", BitMap::level_nodes_count(3), (bitmap.calculate_block_offset(3, 1000) - bitmap.size_in_bytes() - BitMap::level_nodes_count(2) * BitMap::BLOCK_SIZE_IN_BYTES) / BitMap::BLOCK_SIZE_IN_BYTES);

        // bitmap.reset_many(&[1024 - 64]);
        // bitmap.print_leafs();

        // println!("bitmap: {:?}", bitmap);
        // bitmap.print_tree();

        let mut bitmap = BitMap::new(4096 + 512);

        for _ in 0..10 {
            bitmap.set_many(64);
        }

        bitmap.print_tree();
        let flipped = bitmap.set_one();
        bitmap.print_tree();
        println!("flipped: {:?}", flipped);

        bitmap.reset_one(flipped.unwrap());

        bitmap.print_leafs();
        bitmap.print_tree();

        // println!("flipped: {:?}", bitmap.set_many(60));
        // println!("flipped: {:?}", bitmap.set_one());

        // println!("stack: {:?}", bitmap.reset_many(&[4000]));

        // println!("flipped: {:?}", bitmap.flip_many(60));

        // for _ in 0..10 {
        //     println!("flipped: {:?}", bitmap.flip_many(64));
        // }

        // println!("flipped: {:?}", bitmap.flip_many(60));
        // println!("flipped: {:?}", bitmap.flip_one());

        Ok(())
    }
}
