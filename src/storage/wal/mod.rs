use std::{cell::RefCell, fs::File, rc::Rc, sync::Arc};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{
    storage::{
        FrameNumber, PageNumber, StorageResult,
        buffer_pool::{self, BufferPool},
        page::Page,
        pager::{self, MemPageRef, Pager},
    },
    utils::{buffer::Buffer, io::BlockIO},
};

pub const WAL_HEADER_SIZE: usize = size_of::<WalHeader>();
pub const WAL_FRAME_HEADER_SIZE: usize = size_of::<WalFrameHeader>();

#[repr(C)]
#[derive(Debug)]
pub struct WalHeader {
    /// Page size
    pub page_size: u32,

    pub checkpoint_seq_num: u32,
    /// Random number incremented with each checkpoint
    pub salt_1: u32,
    /// Random number incremented with each checkpoint
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct WalFrameHeader {
    /// Page number of frame
    page_number: PageNumber,
    /// Size of database in pages. Only for commit records.
    /// For all other records is 0.
    db_size: u32,
    /// Checksum of header
    checksum_header: u32,
    /// Checksum of page
    checksum_page: u32,
}

pub struct WalIndex {
    map: DashMap<PageNumber, FrameNumber>,
}

impl WalIndex {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    pub fn get(&self, page_number: &PageNumber) -> Option<FrameNumber> {
        self.map.get(page_number).map(|v| v.to_owned())
    }

    pub fn insert(&self, page_number: PageNumber, frame_number: FrameNumber) {
        self.map.insert(page_number, frame_number);
    }

    pub fn remove(&self, page_number: &PageNumber) {
        self.map.remove(page_number);
    }
}

trait Wal {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> StorageResult<()>;

    /// Begin a write transaction.
    fn begin_write_tx(&mut self) -> StorageResult<()>;

    /// End a read transaction.
    fn end_read_tx(&self);

    /// End a write transaction.
    fn end_write_tx(&self);

    fn find_frame(&self, page_number: PageNumber) -> Option<FrameNumber>;

    /// Read a frame from the WAL. No header
    fn read_frame(
        &self,
        frame_number: FrameNumber,
        page: MemPageRef,
        buffer_pool: Arc<BufferPool>,
    ) -> StorageResult<MemPageRef>;

    /// Read a raw frame (header included) from the WAL.
    fn read_frame_raw(&self, frame_number: FrameNumber, frame: &mut [u8]) -> StorageResult<()>;

    /// Write a raw frame (header included) from the WAL.
    fn write_frame_raw(
        &mut self,
        buffer_pool: Arc<BufferPool>,
        frame_number: FrameNumber,
        page_number: PageNumber,
        page: &[u8],
    ) -> StorageResult<()>;

    /// Write a frame to the WAL.
    /// db_size is the database size in pages after the transaction finishes.
    /// db_size > 0    -> last frame written in transaction
    /// db_size == 0   -> non-last frame written in transaction
    /// write_counter is the counter we use to track when the I/O operation starts and completes
    fn append_frame(
        &mut self,
        page: MemPageRef,
        db_size: u32,
        // write_counter: Rc<RefCell<usize>>,
    ) -> StorageResult<()>;

    /// Complete append of frames by updating shared wal state. Before this
    /// all changes were stored locally.
    fn finish_append_frames_commit(&mut self) -> StorageResult<()>;

    fn should_checkpoint(&self) -> bool;

    fn checkpoint(
        &mut self,
        pager: &Pager,
        // write_counter: Rc<RefCell<usize>>,
    ) -> StorageResult<()>;

    fn sync(&mut self) -> StorageResult<()>;

    fn get_max_frame_in_wal(&self) -> u64;

    fn get_max_frame(&self) -> u64;

    fn get_min_frame(&self) -> u64;

    fn rollback(&mut self) -> StorageResult<()>;
}

pub enum SyncState {
    NotSyncing,
    Syncing,
}

pub struct WalFile {
    /// Reference to wal file
    io: Arc<BlockIO<File>>,
    /// Reference to buffer pool
    buffer_pool: Arc<BufferPool>,

    /// Wal index
    index: WalIndex,

    /// Size of file in frames to auto-checkpoint
    checkpoint_size: u64,
    /// Size of frame
    frame_size: u16,

    /// State of syncing
    sync_state: Mutex<SyncState>,
}

impl WalFile {
    pub fn new(
        io: Arc<BlockIO<File>>,
        buffer_pool: Arc<BufferPool>,
        checkpoint_size: u64,
    ) -> WalFile {
        let frame_size = (io.page_size + WAL_FRAME_HEADER_SIZE) as u16;

        Self {
            io,
            buffer_pool,
            index: WalIndex::new(),
            checkpoint_size,
            frame_size,
            sync_state: Mutex::new(SyncState::NotSyncing),
        }
    }
}

impl Wal for WalFile {
    fn find_frame(&self, page_number: PageNumber) -> Option<FrameNumber> {
        self.index.get(&page_number)
    }

    fn read_frame_raw(&self, frame_number: FrameNumber, frame: &mut [u8]) -> StorageResult<()> {
        self.io.raw_read(frame_number as usize, frame)?;
        Ok(())
    }

    fn read_frame(
        &self,
        frame_number: FrameNumber,
        page: MemPageRef,
        buffer_pool: Arc<BufferPool>,
    ) -> StorageResult<MemPageRef> {
        // frame number already is an offset to frame now we only need to skip frame header at the beginning to get page.
        let offset = frame_number as usize + WAL_FRAME_HEADER_SIZE;

        // when pager will try to get page it looks in this pattern: CACHE (buffer already allocated) -> WAL (no buffer) -> DISK (no buffer), so we need to allocate new one.
        let mut buf = buffer_pool.get();
        page.set_locked();

        let read_result = self.io.raw_read(offset, &mut buf);

        pager::complete_read_page(read_result, page, buf, buffer_pool)
    }

    fn write_frame_raw(
        &mut self,
        buffer_pool: Arc<BufferPool>,
        frame_number: FrameNumber,
        page_number: PageNumber,
        page: &[u8],
    ) -> StorageResult<()> {
        let offset = frame_number as usize;

        // page[..size_of ::<PageNumber>()].copy_from_slice(&page_number.to_le_bytes());

        self.io.raw_write(offset, page);

        self.index.remove(&page_number);

        Ok(())
    }

    fn append_frame(
            &mut self,
            page: MemPageRef,
            db_size: u32,
            // write_counter: Rc<RefCell<usize>>,
        ) -> StorageResult<()> {
        todo!()
    }
    
    fn begin_read_tx(&mut self) -> StorageResult<()> {
        todo!()
    }
    
    fn begin_write_tx(&mut self) -> StorageResult<()> {
        todo!()
    }
    
    fn end_read_tx(&self) {
        todo!()
    }
    
    fn end_write_tx(&self) {
        todo!()
    }
    
    fn finish_append_frames_commit(&mut self) -> StorageResult<()> {
        todo!()
    }
    
    fn should_checkpoint(&self) -> bool {
        todo!()
    }
    
    fn checkpoint(
        &mut self,
        pager: &Pager,
        // write_counter: Rc<RefCell<usize>>,
    ) -> StorageResult<()> {
        todo!()
    }
    
    fn sync(&mut self) -> StorageResult<()> {
        todo!()
    }
    
    fn get_max_frame_in_wal(&self) -> u64 {
        todo!()
    }
    
    fn get_max_frame(&self) -> u64 {
        todo!()
    }
    
    fn get_min_frame(&self) -> u64 {
        todo!()
    }
    
    fn rollback(&mut self) -> StorageResult<()> {
        todo!()
    }

}
