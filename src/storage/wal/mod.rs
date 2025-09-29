use std::{
    collections::VecDeque,
    fs::File,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::{
    storage::{
        Error, FrameNumber, PageNumber, StorageResult, buffer_pool::LocalBufferPool,
        page::MAX_PAGE_SIZE, pager::MemPageRef,
    },
    utils::{
        self,
        bytes::{byte_swap_u32, get_u32},
        io::BlockIO,
    },
};

pub const WAL_WRITE_LOCK: usize = 0;
pub const WAL_ALL_BUT_WRITE: usize = 1;
pub const WAL_CHECKPOINT_LOCK: usize = 1;
pub const WAL_RECOVER_LOCK: usize = 2;
// pub const WAL_READ_LOCK: usize = 5;
pub const SHARED_MEMORY_LOCKS_NUM: usize = 8;
pub const WAL_READERS_NUM: usize = SHARED_MEMORY_LOCKS_NUM - 3;

pub const WAL_HEADER_SIZE: usize = size_of::<WalHeader>();
pub const WAL_FRAME_HEADER_SIZE: usize = size_of::<WalFrameHeader>();

pub const READER_NOT_USED: usize = 0xffffffff;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WalHeader {
    /// Page size
    pub page_size: u32,
    /// Counter that after each checkpoint is incremented
    pub checkpoint_seq_num: u32,
    /// Random number incremented with each checkpoint
    pub salt_1: u32,
    /// Random number incremented with each checkpoint
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2: u32,
}

impl WalHeader {
    pub fn from_bytes(buffer: &[u8]) -> StorageResult<Self> {
        let mut src = Cursor::new(buffer);
        Ok(Self {
            page_size: get_u32(&mut src)?,
            checkpoint_seq_num: get_u32(&mut src)?,
            salt_1: get_u32(&mut src)?,
            salt_2: get_u32(&mut src)?,
            checksum_1: get_u32(&mut src)?,
            checksum_2: get_u32(&mut src)?,
        })
    }

    pub fn to_bytes(self, buffer: &mut [u8]) {
        buffer[0..4].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.checkpoint_seq_num.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.salt_1.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.salt_2.to_le_bytes());
        buffer[16..20].copy_from_slice(&self.checksum_1.to_le_bytes());
        buffer[20..24].copy_from_slice(&self.checksum_2.to_le_bytes());
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct WalFrameHeader {
    /// Page number of frame
    page_number: PageNumber,
    /// Size of database in pages. Only for commit records.
    /// For all other records is 0.
    db_size: u32,
    /// Checksum of header (copied from `WalHeader`)
    checksum_header: u32,
    /// Checksum of page
    checksum_page: u32,
}

impl WalFrameHeader {
    pub fn from_bytes(buffer: &[u8]) -> StorageResult<Self> {
        let mut src = Cursor::new(buffer);
        Ok(Self {
            page_number: get_u32(&mut src)?,
            db_size: get_u32(&mut src)?,
            checksum_header: get_u32(&mut src)?,
            checksum_page: get_u32(&mut src)?,
        })
    }

    pub fn to_bytes(self, buffer: &mut [u8]) {
        buffer[0..4].copy_from_slice(&self.page_number.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.db_size.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.checksum_header.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.checksum_page.to_le_bytes());
    }
}

/// Used during checkpoint.
#[derive(Debug)]
pub struct WalCheckpointInfo {
    /// Number of frames backifilled into DB
    num_backfill: u32,
    /// Readers mark
    reader_mark: [u32; WAL_READERS_NUM],
    /// Reserved space for locks
    locks: [u8; SHARED_MEMORY_LOCKS_NUM],
    /// Number of WAL frames that where attempted to be written to DB
    num_backfill_attemped: u32,
}

pub struct Wal {
    /// Database and WAL file handles
    db_file: Arc<BlockIO<File>>,
    wal_file: Arc<BlockIO<File>>,

    /// Truncate WAL to this size upon reset
    max_wal_size: u64,
    frame_size: u32,
    /// Which read lock is being held.  -1 for none
    read_lock: i16,
    /// Flags to use to sync header writes
    sync_flags: u8,
    /// True if in a write transaction
    write_lock: bool,
    /// True if holding checkpoint lock
    checkpoint_lock: bool,
    /// True to truncate WAL file on commit
    truncate_on_commit: bool,
    /// Fsync the WAL header if true
    sync_header: bool,

    /// Wal-index header for current transaction
    header: WalIndexHeader,

    /// Ignore all frames before this one
    min_frame: u32,
    /// On commit, recalculate checksums from here
    recalc_checksum_from: u32,
    /// Checkpoint sequence counter in the wal-header
    checkpoint_counter: u32,
}

pub struct WalIndexHeader {
    transaction_counter: u32,
    max_frame: u32,
    db_size: u32,
    last_frame_checksum: [u32; 2],
    salt: [u32; 2],
    fields_checksum: [u32; 2],
}

pub struct WalIndex {
    header: WalIndexHeader,
    map: DashMap<PageNumber, VecDeque<FrameNumber>>,
}

impl WalIndex {
    pub fn new(header: WalIndexHeader) -> Self {
        Self {
            header,
            map: DashMap::new(),
        }
    }

    pub fn latest_frames_sorted(&self) -> Vec<(PageNumber, FrameNumber)> {
        let mut vec: Vec<_> = self
            .map
            .iter()
            .filter_map(|entry| {
                let page_number = *entry.key();
                let latest = entry.value().back().copied()?;
                Some((page_number, latest))
            })
            .collect();

        vec.sort_by_key(|(pn, _)| *pn);

        vec
    }
}

pub fn wal_checksum_bytes(data: &[u8], initial_checksum: Option<[u32; 2]>) -> [u32; 2] {
    let mut s1 = 0;
    let mut s2 = 0;

    if let Some(i) = initial_checksum {
        s1 = i[0];
        s2 = i[1];
    }

    let len = data.len();

    assert!(
        len >= 8 && len % 8 == 0 && len <= MAX_PAGE_SIZE,
        "{} needs to be divisible by 8",
        len
    );

    // we need to cast it into u32
    let data = utils::cast::cast_slice(data);

    for chunk in data.chunks(2) {
        let a = chunk[0];
        let b = chunk[1];

        s1 = s1.wrapping_add(byte_swap_u32(a).wrapping_add(s2));
        s2 = s2.wrapping_add(byte_swap_u32(b).wrapping_add(s1));
    }

    [s1, s2]
}

// impl WalIndex {
//     pub fn new() -> Self {
//         Self {
//             map: DashMap::new(),
//         }
//     }

//     pub fn get_latest_visible(
//         &self,
//         page_number: &PageNumber,
//         snapshot: FrameNumber,
//     ) -> Option<FrameNumber> {
//         self.map
//             .get(page_number)
//             .and_then(|vec| vec.iter().rev().find(|&&value| value <= snapshot).copied())
//     }

//     pub fn insert(&self, page_number: PageNumber, frame_number: FrameNumber) {
//         self.map
//             .entry(page_number)
//             .or_default()
//             .push_back(frame_number);
//     }

//     /// Drop all frame numbers after checkpoint.
//     pub fn prune(&self, cutoff: FrameNumber) {
//         for mut vec in self.map.iter_mut() {
//             while let Some(&front) = vec.front() {
//                 if front <= cutoff {
//                     vec.pop_front();
//                 } else {
//                     break;
//                 }
//             }
//         }
//     }
// }

// pub struct WalManager {
//     manager: Arc<SharedWal>,
// }

// impl WalManager {
//     pub fn new(file: File, page_size: usize, checkpoint_size: usize) -> StorageResult<Self> {
//         let manager = SharedWal::open(file, page_size, checkpoint_size)?;
//         Ok(Self {
//             manager: Arc::new(manager),
//         })
//     }

//     pub fn local_wal(&self) -> LocalWal {
//         LocalWal::new(self.manager.clone())
//     }
// }

// fn calculate_last_frame(file_size: usize, frame_size: u32) -> FrameNumber {
//     ((file_size - WAL_HEADER_SIZE) / frame_size as usize) as u32
// }

// /// Returns offset in bytes to frame at given number.
// fn calculate_offset_to_frame(frame_size: usize, frame_number: FrameNumber) -> usize {
//     WAL_HEADER_SIZE + frame_size * frame_number as usize
// }

// /// Returns offset in bytes to frame page at given frame number.
// fn calculate_offset_to_page(frame_size: usize, frame_number: FrameNumber) -> usize {
//     calculate_offset_to_frame(frame_size, frame_number) + WAL_FRAME_HEADER_SIZE
// }

// fn valid_frame(
//     io: &BlockIO<File>,
//     frame_number: FrameNumber,
//     checksum: u32,
// ) -> StorageResult<bool> {
//     let mut buffer = [0; WAL_FRAME_HEADER_SIZE];

//     io.read(frame_number, &mut buffer)?;

//     let frame_file_checksum = WalFrameHeader::from_bytes(&mut buffer)?.checksum_header;

//     Ok(frame_file_checksum == checksum)
// }

// /// Shared struct to manage WAL.
// pub struct SharedWal {
//     /// Holds reference to WAL file
//     io: BlockIO<File>,
//     /// Global index of not checkpointed frames to speed up searching
//     index: WalIndex,
//     /// Size of frame in WAL
//     frame_size: usize,
//     /// Size of WAL to auto checkpoint
//     checkpoint_size: usize,
//     checkpoint_seq: AtomicU32,
//     /// First frame number that transaction can see
//     min_frame: AtomicU32,
//     /// Last frame that was commited in transaction
//     max_frame: AtomicU32,
//     /// State of checkpoint
//     sync_state: Mutex<bool>,
//     sync_cv: Condvar,

//     wal_header: Arc<Mutex<WalHeader>>,

//     readers_count: AtomicUsize,
//     writer_lock: Mutex<()>,
// }

// impl SharedWal {
//     fn new(file: File, page_size: usize, checkpoint_size: usize) -> StorageResult<Self> {
//         let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
//         let io = BlockIO::new(file, frame_size, WAL_HEADER_SIZE);

//         let mut wal_buffer = [0; WAL_HEADER_SIZE];
//         io.read_header(&mut wal_buffer)?;

//         let wal_header = WalHeader::from_bytes(&wal_buffer)?;

//         if wal_header.checksum_1 != wal_header.checksum_2 {
//             return Err(Error::InvalidChecksum);
//         }

//         Ok(Self {
//             io,
//             index: WalIndex::new(),
//             frame_size,
//             checkpoint_size,
//             checkpoint_seq: AtomicU32::new(wal_header.checkpoint_seq_num),
//             min_frame: AtomicU32::new(0),
//             max_frame: AtomicU32::new(0),
//             sync_state: Mutex::new(false),
//             sync_cv: Condvar::new(),
//             wal_header: Arc::new(Mutex::new(wal_header)),
//             readers_count: AtomicUsize::new(0),
//             writer_lock: Mutex::new(()),
//         })
//     }

//     pub fn open(file: File, page_size: usize, checkpoint_size: usize) -> StorageResult<Self> {
//         let shared_wal = Self::new(file, page_size, checkpoint_size)?;
//         let wal_header_checksum = unsafe {
//             shared_wal
//                 .wal_header
//                 .data_ptr()
//                 .as_mut()
//                 .unwrap()
//                 .checksum_1
//         };

//         let mut max_frame = 0;
//         let mut count_to_transaction = 0;

//         while let Ok(frame) = shared_wal.read_frame_header(max_frame + count_to_transaction) {
//             if wal_header_checksum != frame.checksum_header {
//                 break;
//             } else if frame.db_size != 0 {
//                 max_frame += count_to_transaction;
//                 count_to_transaction = 0;
//             }
//             count_to_transaction += 1;
//         }

//         unsafe { shared_wal.max_frame.as_ptr().write(max_frame) };

//         Ok(shared_wal)
//     }

//     fn read_frame_header(&self, frame_number: FrameNumber) -> StorageResult<WalFrameHeader> {
//         let mut buffer = [0; WAL_FRAME_HEADER_SIZE];
//         self.io.read(frame_number, &mut buffer)?;

//         WalFrameHeader::from_bytes(&buffer)
//     }

//     /// Begins checkpoint by waiting if necessary for other thread to finish syncing.
//     pub fn begin_checkpoint(&self) {
//         let mut syncing = self.sync_state.lock();

//         // if other thread is syncing then wait
//         if *syncing {
//             self.sync_cv.wait(&mut syncing);
//         }

//         *syncing = true;
//     }

//     /// Ends checkpoint and notifies other threads.
//     pub fn end_checkpoint(&self) {
//         let mut syncing = self.sync_state.lock();
//         *syncing = false;

//         self.sync_cv.notify_all();
//     }
// }

// pub struct LocalWal {
//     /// Reference to shared WAL
//     shared: Arc<SharedWal>,
//     /// Copy of WAL header
//     header: WalHeader,
//     /// Min frame number this handler can see.
//     min_frame: FrameNumber,
//     /// Max frame number this handler can see.
//     max_frame: FrameNumber,
// }

// impl LocalWal {
//     pub fn new(shared: Arc<SharedWal>) -> Self {
//         let header = shared.wal_header.lock().clone();
//         Self {
//             shared,
//             header,
//             min_frame: 0,
//             max_frame: 0,
//         }
//     }

//     pub fn begin_read_tx(&mut self) -> StorageResult<()> {
//         self.min_frame = self.shared.min_frame.load(Ordering::Acquire);
//         self.max_frame = self.shared.max_frame.load(Ordering::Acquire);

//         self.shared.readers_count.fetch_add(1, Ordering::SeqCst);

//         Ok(())
//     }

//     pub fn begin_write_tx(&mut self) -> StorageResult<MutexGuard<()>> {
//         let guard = self.shared.writer_lock.lock();

//         Ok(guard)
//     }

//     pub fn end_read_tx(&mut self) {
//         self.shared.readers_count.fetch_sub(1, Ordering::SeqCst);
//     }

//     pub fn end_write_tx(&mut self, guard: MutexGuard<()>) {
//         drop(guard);
//     }

//     pub fn commit(&mut self) {

//     }

// pub fn append_frame(&self, page: MemPageRef, db_size: u32) -> StorageResult<FrameNumber> {
//     let page_number = page.get().id;
//     let frame_number = self.shared.max_frame.fetch_add(1, Ordering::SeqCst) as FrameNumber;

//     let page_data = page.get_content().as_ptr();
//     let checksum_page = utils::bytes::checksum_crc32(&page_data);

//     let header = WalFrameHeader {
//         page_number,
//         db_size,
//         checksum_header: self.wal_header.lock().checksum_1,
//         checksum_page,
//     };

//     let mut frame_buffer = vec![0; self.frame_size];
//     header.to_bytes(&mut frame_buffer);
//     frame_buffer[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&page_data);

//     self.io.write(frame_number, &frame_buffer)?;

//     Ok(frame_number)
// }

// pub fn read_frame(&self, frame_number: FrameNumber, page: MemPageRef, buffer_pool: LocalBufferPool) -> StorageResult<MemPageRef> {

// }
// }

// trait Wal {
//     /// Begin a read transaction.
//     fn begin_read_tx(&mut self) -> StorageResult<()>;

//     /// Begin a write transaction.
//     fn begin_write_tx(&mut self) -> StorageResult<()>;

//     /// End a read transaction.
//     fn end_read_tx(&self);

//     /// End a write transaction.
//     fn end_write_tx(&self);

//     fn find_frame(&self, page_number: PageNumber) -> Option<FrameNumber>;

//     /// Read a frame from the WAL. No header
//     fn read_frame(
//         &self,
//         frame_number: FrameNumber,
//         page: MemPageRef,
//         buffer_pool: LocalBufferPool,
//     ) -> StorageResult<MemPageRef>;

//     /// Read a raw frame (header included) from the WAL.
//     fn read_frame_raw(&self, frame_number: FrameNumber, frame: &mut [u8]) -> StorageResult<()>;

//     /// Write a raw frame (header included) from the WAL.
//     fn write_frame_raw(
//         &mut self,
//         frame_number: FrameNumber,
//         page_number: PageNumber,
//         frame: &[u8],
//     ) -> StorageResult<()>;

//     /// Write a frame to the WAL.
//     /// db_size is the database size in pages after the transaction finishes.
//     /// db_size > 0    -> last frame written in transaction
//     /// db_size == 0   -> non-last frame written in transaction
//     /// write_counter is the counter we use to track when the I/O operation starts and completes
//     fn append_frame(
//         &mut self,
//         page: MemPageRef,
//         db_size: u32,
//         // write_counter: Rc<RefCell<usize>>,
//     ) -> StorageResult<()>;

//     /// Complete append of frames by updating shared wal state. Before this
//     /// all changes were stored locally.
//     fn finish_append_frames_commit(&mut self) -> StorageResult<()>;

//     fn should_checkpoint(&self) -> bool;

//     fn checkpoint(
//         &mut self,
//         pager: &Pager,
//         // write_counter: Rc<RefCell<usize>>,
//     ) -> StorageResult<()>;

//     fn sync(&mut self) -> StorageResult<()>;

//     fn get_max_frame_in_wal(&self) -> u64;

//     fn get_max_frame(&self) -> u64;

//     fn get_min_frame(&self) -> u64;

//     fn rollback(&mut self) -> StorageResult<()>;
// }

// pub struct WalFile {
//     /// Reference to wal file
//     io: Arc<BlockIO<File>>,
//     /// Wal index
//     index: WalIndex,
//     /// Size of file in frames to auto-checkpoint
//     checkpoint_size: u64,
//     /// Last frame in WAL
//     last_frame: u32,
//     /// Size of frame
//     frame_size: u16,
//     /// State of syncing
//     sync_state: Mutex<SyncState>,
//     /// Simple pre-allocated buffer to be reused durring write operations
//     write_buffer: Vec<u8>,
// }

// impl WalFile {
//     pub fn new(io: Arc<BlockIO<File>>, checkpoint_size: u64) -> WalFile {
//         let frame_size = (io.page_size + WAL_FRAME_HEADER_SIZE) as u16;

//         Self {
//             io,
//             index: WalIndex::new(),
//             checkpoint_size,
//             max_frame: Self::calculate_last_frame(io., frame_size)
//             frame_size,
//             sync_state: Mutex::new(SyncState::NotSyncing),
//             write_buffer: vec![0; frame_size as usize],
//         }
//     }

//     fn calculate_last_frame(file_size: usize, frame_size: u32) -> FrameNumber {
//         ((file_size - WAL_HEADER_SIZE) / frame_size as usize) as u32
//     }

//     /// Returns offset in bytes to frame at given number.
//     fn calculate_offset_to_frame(&self, frame_number: FrameNumber) -> usize {
//         WAL_HEADER_SIZE + self.frame_size as usize * frame_number as usize
//     }

//     /// Returns offset in bytes to frame page at given frame number.
//     fn calculate_offset_to_page(&self, frame_number: FrameNumber) -> usize {
//         self.calculate_offset_to_frame(frame_number) + WAL_FRAME_HEADER_SIZE
//     }

//     /// Writes page content to `write_buffer` as frame.
//     fn serialize_page(&mut self, page_number: PageNumber, db_size: u32, page_content: &[u8]) {
//         self.write_buffer[WAL_FRAME_HEADER_SIZE..].copy_from_slice(page_content);

//         let header = WalFrameHeader {
//             page_number,
//             db_size,
//             checksum_header: 0,
//             checksum_page: 0,
//         };

//         header.write_to_buffer(&mut self.write_buffer);
//     }
// }

// impl Wal for WalFile {
//     fn find_frame(&self, page_number: PageNumber) -> Option<FrameNumber> {
//         self.index.get(&page_number)
//     }

//     fn read_frame_raw(&self, frame_number: FrameNumber, frame: &mut [u8]) -> StorageResult<()> {
//         self.io.raw_read(frame_number as usize, frame)?;
//         Ok(())
//     }

//     fn read_frame(
//         &self,
//         frame_number: FrameNumber,
//         page: MemPageRef,
//         buffer_pool: LocalBufferPool,
//     ) -> StorageResult<MemPageRef> {
//         let offset = self.calculate_offset_to_page(frame_number);

//         // when pager will try to get page it looks in this pattern: CACHE (buffer already allocated) -> WAL (no buffer) -> DISK (no buffer), so we need to allocate new one.
//         let mut buf = buffer_pool.get();
//         page.set_locked();

//         let read_result = self.io.raw_read(offset, &mut buf[..]);

//         pager::complete_read_page(read_result, page, buf)
//     }

//     fn write_frame_raw(
//         &mut self,
//         frame_number: FrameNumber,
//         page_number: PageNumber,
//         frame: &[u8],
//     ) -> StorageResult<()> {
//         let offset = self.calculate_offset_to_frame(frame_number);

//         self.io.raw_write(offset, frame)?;

//         self.index.remove(&page_number);

//         Ok(())
//     }

//     fn append_frame(
//         &mut self,
//         page: MemPageRef,
//         db_size: u32,
//         // write_counter: Rc<RefCell<usize>>,
//     ) -> StorageResult<()> {
//         page.set_locked();
//         let page_number = page.get().id;

//         self.serialize_page(page_number, db_size, &page.get_content().as_ptr());

//         self.write_frame_raw(frame_number, page_number, frame)

//         todo!();
//     }

//     fn begin_read_tx(&mut self) -> StorageResult<()> {
//         todo!()
//     }

//     fn begin_write_tx(&mut self) -> StorageResult<()> {
//         todo!()
//     }

//     fn end_read_tx(&self) {
//         todo!()
//     }

//     fn end_write_tx(&self) {
//         todo!()
//     }

//     fn finish_append_frames_commit(&mut self) -> StorageResult<()> {
//         todo!()
//     }

//     fn should_checkpoint(&self) -> bool {
//         todo!()
//     }

//     fn checkpoint(
//         &mut self,
//         pager: &Pager,
//         // write_counter: Rc<RefCell<usize>>,
//     ) -> StorageResult<()> {
//         todo!()
//     }

//     fn sync(&mut self) -> StorageResult<()> {
//         todo!()
//     }

//     fn get_max_frame_in_wal(&self) -> u64 {
//         todo!()
//     }

//     fn get_max_frame(&self) -> u64 {
//         todo!()
//     }

//     fn get_min_frame(&self) -> u64 {
//         todo!()
//     }

//     fn rollback(&mut self) -> StorageResult<()> {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum() {
        let data: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];

        let checksum = wal_checksum_bytes(&data, None);
        println!("Checksum: {} {}", checksum[0], checksum[1]);

        let data: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        let checksum = wal_checksum_bytes(&data, None);
        println!("Checksum: {} {}", checksum[0], checksum[1]);
    }
}
