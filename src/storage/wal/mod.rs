use std::{
    collections::VecDeque,
    fs::File,
    io::{IoSlice, Write},
    mem::ManuallyDrop,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::{
    database::MemDatabaseHeader,
    os::{Open, OpenOptions},
    storage::{
        self, Error, FrameNumber, PageNumber,
        buffer_pool::BufferPool,
        cache::ShardedClockCache,
        page::{MAX_PAGE_SIZE, Page},
        pager::{self, ExclusivePageGuard},
        wal::{
            locks::{PackedU64, ReadGuard, ReadersPool, WriteGuard},
            transaction::{ReadTransaction, ReadTx, WriteTransaction, WriteTx},
        },
    },
    utils::{
        self,
        bytes::{byte_swap_u32, pack_u64},
        concurrency::Semaphore,
        io::{BlockIO, FileOps, IO},
    },
};

mod locks;
pub mod transaction;

const WAL_HEADER_SIZE: usize = size_of::<WalHeader>();
const WAL_HEADER_SIZE_NO_CHECKSUM: usize = WAL_HEADER_SIZE - size_of::<[u32; 2]>();
const FRAME_HEADER_SIZE: usize = size_of::<FrameHeader>();
const READERS_NUM: usize = 5;

const DEFAULT_CHECKPOINT_SIZE: FrameNumber = 1000;

type Checksum = (u32, u32);

#[repr(C)]
#[derive(Debug)]
pub struct WalHeader {
    /// Size of invidual page in wal (not frame).
    page_size: u32,
    /// Incremented with each checkpoint.
    checkpoint_seq_num: u32,
    /// Frame number of last checkpointed entry (if WAL is empty, then it is set to 0).
    last_checkpointed: u32,
    /// Database size in pages (updated after each checkpoint).
    db_size: u32,
    /// Number of frames transfered from WAL to DB.
    backfilled_number: u32,
    /// Padding to make header size divisible by 8.
    padding: [u8; 4],
    /// Checksum of header
    checksum: Checksum,
}

impl WalHeader {
    /// Recalculates checksum of all fields except `checksum`.
    pub fn update_checksum(&mut self) {
        self.checksum = self.checksum_self(None);
    }

    pub fn default(page_size: u32, db_size: u32) -> Self {
        let mut wal_header = Self {
            page_size,
            checkpoint_seq_num: 0,
            last_checkpointed: 0,
            db_size,
            backfilled_number: 0,
            checksum: (0, 0),
            padding: [0; 4],
        };
        wal_header.update_checksum();
        wal_header
    }

    pub fn from_bytes(buffer: &[u8]) -> Self {
        let page_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let checkpoint_seq_num = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let last_checkpointed = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let db_size = u32::from_le_bytes(buffer[12..16].try_into().unwrap());
        let backfilled_number = u32::from_le_bytes(buffer[16..20].try_into().unwrap());
        let padding = buffer[20..24].try_into().unwrap();
        let checksum = (
            u32::from_le_bytes(buffer[24..28].try_into().unwrap()),
            u32::from_le_bytes(buffer[28..32].try_into().unwrap()),
        );

        Self {
            page_size,
            checkpoint_seq_num,
            last_checkpointed,
            db_size,
            backfilled_number,
            checksum,
            padding,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0; WAL_HEADER_SIZE];
        self.write_to_buffer(&mut buffer);
        buffer
    }

    pub fn write_to_buffer(&self, buffer: &mut [u8]) {
        buffer[0..4].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.checkpoint_seq_num.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.last_checkpointed.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.db_size.to_le_bytes());
        buffer[16..20].copy_from_slice(&self.backfilled_number.to_le_bytes());
        buffer[20..24].copy_from_slice(&self.padding);
        buffer[24..28].copy_from_slice(&self.checksum.0.to_le_bytes());
        buffer[28..32].copy_from_slice(&self.checksum.1.to_le_bytes());
    }

    fn checksum_self(&self, seed: Option<Checksum>) -> Checksum {
        let header_bytes = &utils::cast::bytes_of(self)[..WAL_HEADER_SIZE_NO_CHECKSUM];
        checksum_bytes(header_bytes, seed)
    }
}

pub struct MemWalHeader {
    pub page_size: u32,
    checkpoint_seq_num: AtomicU32,
    last_checkpointed: AtomicU32,
    db_size: AtomicU32,
    backfilled_number: AtomicU32,
    checksum: AtomicU64,
}

impl MemWalHeader {
    pub fn into_raw_header(&self) -> WalHeader {
        WalHeader {
            page_size: self.page_size,
            checkpoint_seq_num: self.get_checkpoint_seq_num(),
            last_checkpointed: self.get_last_checkpointed(),
            db_size: self.get_db_size(),
            backfilled_number: self.get_backfilled_number(),
            checksum: self.get_checksum(),
            padding: [0; 4],
        }
    }

    pub fn update_checksum(&self) {
        let checksum = self.into_raw_header().checksum_self(None);
        self.set_checksum(checksum);
    }

    pub fn get_checkpoint_seq_num(&self) -> u32 {
        self.checkpoint_seq_num.load(Ordering::Acquire)
    }

    pub fn get_last_checkpointed(&self) -> u32 {
        self.last_checkpointed.load(Ordering::Acquire)
    }

    pub fn get_db_size(&self) -> u32 {
        self.db_size.load(Ordering::Acquire)
    }

    pub fn get_backfilled_number(&self) -> u32 {
        self.backfilled_number.load(Ordering::Acquire)
    }

    pub fn get_checksum(&self) -> Checksum {
        self.checksum.load_packed(Ordering::Acquire)
    }

    pub fn increment_checkpoint_seq_num(&self) {
        self.checkpoint_seq_num.fetch_add(1, Ordering::Release);
    }

    pub fn set_last_checkpointed(&self, value: PageNumber) {
        self.last_checkpointed.store(value, Ordering::Release);
    }

    pub fn set_db_size(&self, value: u32) {
        self.db_size.store(value, Ordering::Release);
    }

    pub fn set_backfilled_number(&self, value: u32) {
        self.backfilled_number.store(value, Ordering::Release);
    }

    pub fn set_checksum(&self, (val_1, val_2): Checksum) {
        self.checksum.store_packed(val_1, val_2, Ordering::Release);
    }
}

impl From<WalHeader> for MemWalHeader {
    fn from(wal_header: WalHeader) -> Self {
        Self {
            page_size: wal_header.page_size,
            checkpoint_seq_num: AtomicU32::new(wal_header.checkpoint_seq_num),
            last_checkpointed: AtomicU32::new(wal_header.last_checkpointed),
            db_size: AtomicU32::new(wal_header.db_size),
            backfilled_number: AtomicU32::new(wal_header.backfilled_number),
            checksum: AtomicU64::new(pack_u64(wal_header.checksum.0, wal_header.checksum.1)),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FrameHeader {
    /// Page number of frame
    page_number: PageNumber,
    /// Size of database in pages. Only for commit records.
    /// For all other records is 0.
    db_size: u32,
    /// Checksum of page (calculated by using previous frames)
    checksum: Checksum,
}

impl FrameHeader {
    pub fn from_bytes(buffer: &[u8]) -> Self {
        let page_number = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let db_size = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let checksum = (
            u32::from_le_bytes(buffer[8..12].try_into().unwrap()),
            u32::from_le_bytes(buffer[12..16].try_into().unwrap()),
        );

        Self {
            page_number,
            db_size,
            checksum,
        }
    }

    pub fn to_bytes(&self, buffer: &mut [u8]) {
        buffer[0..4].copy_from_slice(&self.page_number.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.db_size.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.checksum.0.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.checksum.1.to_le_bytes());
    }
}

pub struct WalManager {
    global_wal: Arc<GlobalWal>,
}

impl WalManager {
    pub fn new(
        wal_file_path: PathBuf,
        db_file: Arc<BlockIO<File>>,
        page_size: u32,
        db_size: u32,
        page_cache: Arc<ShardedClockCache>,
    ) -> storage::Result<Self> {
        let file_exists = wal_file_path.exists();

        let mut file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(false)
            .truncate(false)
            .lock(true)
            .open(wal_file_path)?;

        if !file_exists {
            let buf = &mut [0; WAL_HEADER_SIZE];
            let default_header = WalHeader::default(page_size, db_size);
            default_header.write_to_buffer(buf);

            file.pwrite(0, buf)?;
            file.flush()?;
            file.sync()?;
        }

        let header = {
            let buf = &mut [0; WAL_HEADER_SIZE];
            file.pread(0, buf)?;
            let header = WalHeader::from_bytes(buf);

            // validate header checksum
            if header.checksum != header.checksum_self(None) {
                return Err(Error::InvalidChecksum);
            }
            header
        };

        let wal_file = Arc::new(BlockIO::new(file, page_size as usize, WAL_HEADER_SIZE));

        let global_wal = GlobalWal::new(wal_file, db_file, header, page_cache)?;

        Ok(Self {
            global_wal: Arc::new(global_wal),
        })
    }

    pub fn local_wal(&self) -> LocalWal {
        LocalWal::new(self.global_wal.clone())
    }
}

pub struct GlobalWal {
    wal_file: Arc<BlockIO<File>>,
    db_file: Arc<BlockIO<File>>,

    frame_size: usize,

    /// Size of WAL in pages after which we should trigger checkpoint.
    checkpoint_size: u32,
    /// WAL header loaded to memory.
    header: Arc<MemWalHeader>,
    /// WAL index that sppeds up searching for frame.
    index: Arc<WalIndex>,
    /// Reference to page cache used durring checkpointing to dump dirty pages.
    page_cache: Arc<ShardedClockCache>,
    // /// Last checkpointed entry in WAL.
    // min_frame: AtomicU32,
    /// Last commited frame in transaction.
    max_frame: AtomicU32,
    /// Checksum of last frame in WAL. It is cumulative checksum of all pages. Stored as two u32 packed in single atomic.
    last_checksum: AtomicU64,

    // /// Number of frames transfered to DB from WAL.
    // backfilled_number: AtomicU32,
    // /// Counter of checkpoints
    // checkpoint_seq_num: AtomicU32,
    /// Array of read locks. Each locks is atomic u32 which represents minimal frame number this transaction can see.
    readers: Arc<ReadersPool<READERS_NUM>>,
    /// Lock for single writer.
    writer: Mutex<()>,
    /// All transactions read or write must acquire this lock before they can do anything.
    /// When we want to run checkpoint we get write lock instead to block other transactions.
    /// While this can couse latency spikes, it prevents WAL from growing indefinitely.
    checkpoint_lock: RwLock<()>,
}

impl GlobalWal {
    pub fn new(
        wal_file: Arc<BlockIO<File>>,
        db_file: Arc<BlockIO<File>>,
        header: WalHeader,
        page_cache: Arc<ShardedClockCache>,
    ) -> storage::Result<Self> {
        let frame_size = header.page_size as usize + FRAME_HEADER_SIZE;
        // let backfilled_number = header.backfilled_number;
        // let checkpoint_seq_num = header.checkpoint_seq_num;
        let index = Arc::new(WalIndex::new());

        // let min_frame = header.last_checkpointed;
        let mut max_frame = 0;
        let mut transaction_frames = vec![];

        let mut running_checksum = header.checksum_self(None);
        let mut buffer = vec![0; frame_size];

        for frame_number in 1..header.last_checkpointed {
            let (valid, is_commit, page_number) =
                validate_frame(&wal_file, frame_number, &mut running_checksum, &mut buffer)?;
            if !valid {
                break;
            }

            transaction_frames.push((page_number, frame_number));

            if is_commit {
                for (pn, f) in transaction_frames.drain(..) {
                    index.insert_latest(pn, f);
                }
                max_frame = frame_number;
            }
        }

        Ok(Self {
            wal_file,
            db_file,
            frame_size,
            checkpoint_size: DEFAULT_CHECKPOINT_SIZE,
            header: Arc::new(MemWalHeader::from(header)),
            index,
            page_cache,
            // min_frame: AtomicU32::new(min_frame),
            max_frame: AtomicU32::new(max_frame),
            last_checksum: AtomicU64::new(pack_u64(running_checksum.0, running_checksum.1)),
            // backfilled_number: AtomicU32::new(backfilled_number),
            // checkpoint_seq_num: AtomicU32::new(checkpoint_seq_num),
            // by default it is set to u32::MAX to indicate that it is free and it is quite not possible for WAL to outgrow 17 TB of 4KB pages.
            readers: Arc::new(ReadersPool::new()),
            writer: Mutex::new(()),
            checkpoint_lock: RwLock::new(()),
        })
    }

    pub fn write_header(&self) -> storage::Result<()> {
        let raw_header = self.header.into_raw_header();

        self.wal_file.write_header(&raw_header.to_bytes())?;

        Ok(())
    }

    pub fn get_min_frame(&self) -> FrameNumber {
        self.header.get_last_checkpointed()
    }

    pub fn get_max_frame(&self) -> FrameNumber {
        self.max_frame.load(Ordering::Acquire)
    }

    pub fn get_last_checksum(&self) -> Checksum {
        self.last_checksum.load_packed(Ordering::Acquire)
    }

    pub fn get_backfilled_number(&self) -> u32 {
        self.header.get_backfilled_number()
    }

    pub fn get_checkpoint_seq_num(&self) -> u32 {
        self.header.get_checkpoint_seq_num()
    }

    pub fn set_min_frame(&self, value: FrameNumber) {
        self.header.set_last_checkpointed(value);
    }

    pub fn set_max_frame(&self, value: FrameNumber) {
        self.max_frame.store(value, Ordering::Release);
    }

    pub fn set_last_checksum(&self, value: Checksum) {
        self.last_checksum
            .store_packed(value.0, value.1, Ordering::Release);
    }

    pub fn set_backfilled_number(&self, value: u32) {
        self.header.set_backfilled_number(value);
    }

    pub fn increment_checkpoint_seq_num(&self) {
        self.header.increment_checkpoint_seq_num();
    }
}

pub struct LocalWal {
    /// Reference to `GlobalWal`
    global_wal: Arc<GlobalWal>,
    /// Copied from `GlobalWal` when beginning transaction.
    min_frame: u32,
    /// Copied from `GlobalWal` when beginning transaction.
    max_frame: u32,
    /// Copied from `GlobalWal` when beginning transaction.
    last_checksum: Checksum,
}

impl LocalWal {
    pub fn new(global_wal: Arc<GlobalWal>) -> Self {
        Self {
            global_wal,
            min_frame: 0,
            max_frame: 0,
            last_checksum: (0, 0),
        }
    }

    fn calculate_frame_offset(&self, frame_number: FrameNumber) -> storage::Result<usize> {
        self.global_wal
            .wal_file
            .calculate_offset(frame_number)
            .map_err(|err| err.into())
    }

    pub fn is_in_wal(&self, page_number: &PageNumber) -> bool {
        self.global_wal.index.contains(page_number)
    }

    pub fn begin_read_tx(&mut self) -> storage::Result<ReadGuard<READERS_NUM>> {
        let checkpoint_guard = self.global_wal.checkpoint_lock.read();

        let guard = self
            .global_wal
            .readers
            .clone()
            .acquire(checkpoint_guard, || self.global_wal.get_min_frame());

        // takes snapshot of both min and max frame that this transaction will be able to see in WAL
        self.min_frame = guard.min_frame();
        self.max_frame = self.global_wal.get_max_frame();

        Ok(guard)
    }

    pub fn begin_write_tx(&mut self) -> storage::Result<WriteGuard> {
        let checkpoint_guard = self.global_wal.checkpoint_lock.read();
        let mutex_guard = self.global_wal.writer.lock();

        let guard = WriteGuard::new(checkpoint_guard, mutex_guard);

        self.min_frame = self.global_wal.get_min_frame();
        self.max_frame = self.global_wal.get_max_frame();
        self.last_checksum = self.global_wal.get_last_checksum();

        Ok(guard)
    }

    pub fn end_read_tx(&mut self, guard: ReadGuard<READERS_NUM>) -> storage::Result<()> {
        self.min_frame = 0;
        self.max_frame = 0;

        drop(guard);

        Ok(())
    }

    /// Ends write transaction by dropping write locks and updates all
    pub fn end_write_tx(&mut self, guard: WriteGuard) -> storage::Result<()> {
        self.global_wal.wal_file.persist()?;

        self.global_wal.set_last_checksum(self.last_checksum);
        self.global_wal.set_max_frame(self.max_frame);

        self.min_frame = 0;
        self.max_frame = 0;

        drop(guard);

        self.checkpoint()?;

        Ok(())
    }

    /// Reads only page from WAL (skips header).
    fn read_raw(&self, frame_number: FrameNumber, buffer: &mut [u8]) -> storage::Result<usize> {
        let offset = self.calculate_frame_offset(frame_number)? + FRAME_HEADER_SIZE;
        self.global_wal
            .wal_file
            .raw_read(offset, buffer)
            .map_err(|err| err.into())
    }

    /// Reads given `page_number` from WAL. This operation doesn't interrupt
    /// other readers, because each reader is bounded to it's min and max frame.
    pub fn read_frame(
        &mut self,
        page_number: PageNumber,
        buffer_pool: &Arc<BufferPool>,
    ) -> storage::Result<Option<Page>> {
        // if given page number is not present in WAL we can simply return OK(None)
        if !self.is_in_wal(&page_number) {
            return Ok(None);
        }

        // pager::begin_read_page(&page)?;

        let visible_frame_number = self
            .global_wal
            .index
            .get(&page_number, self.max_frame)
            .ok_or(Error::PageNotFoundInWal(page_number))?;

        let mut buffer = buffer_pool.get();

        let read_result =
            self.global_wal
                .wal_file
                .read(visible_frame_number, &mut buffer, FRAME_HEADER_SIZE);

        // complete_read_page(&read_result, &page, buffer);

        match read_result {
            Ok(bytes_read) => {
                if bytes_read == buffer.size() {
                    let offset = if page_number == 1 { WAL_HEADER_SIZE } else { 0 };
                    Ok(Some(Page::new(offset, buffer)))
                } else {
                    Err(Error::PageNotFoundInWal(page_number))
                }
            }
            Err(_) => Err(Error::PageNotFoundInWal(page_number)),
        }
    }

    pub fn append_frame(&mut self, page: ExclusivePageGuard, db_size: u32) -> storage::Result<()> {
        self.append_vectored(vec![page], db_size)
    }

    pub fn append_vectored(
        &mut self,
        mut pages: Vec<ExclusivePageGuard>,
        db_size: u32,
    ) -> storage::Result<()> {
        let pages_number = pages.len();

        let mut io_buffers = Vec::with_capacity(pages.len() * 2);
        let mut header_buffers: Vec<Vec<u8>> = (0..pages.len())
            .map(|_| vec![0; FRAME_HEADER_SIZE])
            .collect();

        let mut running_checksum = self.last_checksum;

        for (i, page) in pages.iter().enumerate() {
            // begin_write_page(page)?;

            let page_number = page.id();
            let page_content = page.as_ptr();
            let checksum = checksum_bytes(page_content, Some(running_checksum));

            running_checksum = checksum;
            let commit_db_size = if i + 1 == pages.len() { db_size } else { 0 };

            let frame_header = FrameHeader {
                page_number,
                db_size: commit_db_size,
                checksum,
            };

            frame_header.to_bytes(&mut header_buffers[i][..]);
        }

        for (i, page) in pages.iter().enumerate() {
            io_buffers.push(IoSlice::new(&header_buffers[i]));
            io_buffers.push(page.as_io_slice());
        }

        let frame_number = self.max_frame + 1;

        let write_result = self
            .global_wal
            .wal_file
            .write_vectored(frame_number, &mut io_buffers);

        for page in pages.iter_mut() {
            pager::complete_write_page(&write_result, page, false);
        }

        self.last_checksum = running_checksum;
        self.max_frame = self.max_frame + pages_number as FrameNumber;

        for (i, page) in pages.iter().enumerate() {
            self.global_wal
                .index
                .insert(page.id(), frame_number + i as FrameNumber);
        }

        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        let max_frame = self.global_wal.get_max_frame();
        // I don't think this is usefull for now
        let backfilled_number = self.global_wal.get_backfilled_number() * 0;

        max_frame > self.global_wal.checkpoint_size + backfilled_number
    }

    pub fn checkpoint(&mut self) -> storage::Result<()> {
        if !self.should_checkpoint() {
            return Ok(());
        }

        let exclusive_lock = self.global_wal.checkpoint_lock.write();
        // let mut wal_header = self.global_wal.header.lock();

        let max_frame = self.global_wal.get_max_frame();

        let frames_to_checkpoint = self.global_wal.index.latest_frames_sorted(max_frame);
        let to_backfill = frames_to_checkpoint.len() as u32;
        let mut temp_buffer = vec![0; self.global_wal.frame_size - FRAME_HEADER_SIZE];

        // iterates over pages that should be moved to db file. If they are
        // present in cache and dirty, they are written straight from cache
        for (page_number, frame_number) in frames_to_checkpoint {
            let offset = self.global_wal.db_file.calculate_offset(page_number)?;
            if let Some(mem_page) = self.global_wal.page_cache.get(&page_number) {
                let guard = mem_page.lock_exclusive();
                if guard.is_dirty() {
                    self.global_wal.db_file.raw_write(offset, guard.as_ptr())?;
                    guard.clear_dirty();
                }
            } else {
                self.read_raw(frame_number, &mut temp_buffer)?;
                self.global_wal.db_file.raw_write(offset, &temp_buffer)?;
            }
        }
        // persist changes to db file
        self.global_wal.db_file.persist()?;

        // change wal header params, because it will be truncated
        self.global_wal.set_backfilled_number(to_backfill);
        self.global_wal.set_min_frame(0);
        self.global_wal.set_max_frame(0);
        self.global_wal.set_backfilled_number(to_backfill);
        self.global_wal.increment_checkpoint_seq_num();

        self.global_wal.header.update_checksum();

        // write header to WAL
        self.global_wal.write_header()?;

        // removes all frames from WAL leaving the header
        self.global_wal.wal_file.truncate_beginning(max_frame)?;

        // persist changes to wal file
        self.global_wal.wal_file.persist()?;

        self.global_wal.index.clear();

        drop(exclusive_lock);

        Ok(())
    }
}

#[derive(Debug)]
pub struct WalIndex {
    map: DashMap<PageNumber, VecDeque<FrameNumber>>,
}

impl WalIndex {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Inserts `page_number` with `frame_number` and if they are
    /// already existing frame numbers it removes all of them.
    ///
    /// # Note
    ///
    /// This function should only be used when initializing WAL index.
    fn insert_latest(&self, page_number: PageNumber, frame_number: FrameNumber) {
        let mut entry = self.map.entry(page_number).or_default();
        entry.clear();
        entry.push_back(frame_number);
    }

    /// Inserts new frame of given `page_number`.
    pub fn insert(&self, page_number: PageNumber, frame_number: FrameNumber) {
        self.map
            .entry(page_number)
            .or_default()
            .push_back(frame_number);
    }

    /// Returns sorted list of latest visible frames used for checkpoint to put them into db.
    pub fn latest_frames_sorted(&self, max_frame: FrameNumber) -> Vec<(PageNumber, FrameNumber)> {
        let mut vec: Vec<_> = self
            .map
            .iter()
            .filter_map(|entry| {
                let page_number = *entry.key();
                let latest_visible = entry
                    .value()
                    .iter()
                    .rev()
                    .find(|&&frame| frame <= max_frame)
                    .copied()?;
                Some((page_number, latest_visible))
            })
            .collect();

        vec.sort_by_key(|(pn, _)| *pn);

        vec
    }

    pub fn contains(&self, page_number: &PageNumber) -> bool {
        self.map.contains_key(page_number)
    }

    pub fn get(&self, page_number: &PageNumber, max_frame: FrameNumber) -> Option<FrameNumber> {
        self.map
            .get(page_number)
            .and_then(|vec| vec.iter().rev().find(|&&frame| frame <= max_frame).copied())
    }

    pub fn clear(&self) {
        self.map.clear();
    }
}

/// Validates if given frame is valid and WAL isn't corrupted. If if frame isn't valid,
/// it won't update `running_checksum`. Returns two boolean variables and page number.
/// First bool is true if frame checksum is valid and second bool is true if this frame
///  was commit frame.
///
/// # Note
///
/// Use this function only when scanning WAL from beginning. \
/// Otherwise checksum will not be calculated correctly
fn validate_frame(
    wal_file: &Arc<BlockIO<File>>,
    frame_number: FrameNumber,
    running_checksum: &mut Checksum,
    buffer: &mut [u8],
) -> storage::Result<(bool, bool, PageNumber)> {
    let offset = wal_file.calculate_offset(frame_number)?;
    if wal_file.raw_read(offset, buffer).is_err() {
        return Ok((false, false, 0));
    }

    let frame_header = FrameHeader::from_bytes(buffer);

    // calculates checksum of given frame's page content with previous frame checksum as seed.
    let new_checksum = checksum_bytes(&buffer[FRAME_HEADER_SIZE..], Some(*running_checksum));

    let valid = frame_header.checksum == new_checksum;
    let is_commit = frame_header.db_size != 0;

    if valid {
        *running_checksum = new_checksum;
    }

    Ok((valid, is_commit, frame_header.page_number))
}

pub fn checksum_bytes(data: &[u8], seed: Option<Checksum>) -> Checksum {
    let mut s1 = 0;
    let mut s2 = 0;

    if let Some((seed_1, seed_2)) = seed {
        s1 = seed_1;
        s2 = seed_2;
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

    (s1, s2)
}

pub struct WriteAheadLog {
    wal_file: Arc<BlockIO<File>>,
    db_file: Arc<BlockIO<File>>,

    frame_size: usize,

    /// Size of WAL in pages after which we should trigger checkpoint.
    trigger_checkpoint: u32,
    /// WAL header loaded to memory.
    header: Arc<MemWalHeader>,
    /// WAL index that sppeds up searching for frame.
    index: Arc<WalIndex>,
    /// Reference to page cache used durring checkpointing to dump dirty pages.
    page_cache: Arc<ShardedClockCache>,
    // /// Last checkpointed entry in WAL.
    // min_frame: AtomicU32,
    /// Last commited frame in transaction.
    max_frame: AtomicU32,
    /// Checksum of last frame in WAL. It is cumulative checksum of all pages. Stored as two u32 packed in single atomic.
    last_checksum: AtomicU64,

    // /// Number of frames transfered to DB from WAL.
    // backfilled_number: AtomicU32,
    // /// Counter of checkpoints
    // checkpoint_seq_num: AtomicU32,
    /// Array of read locks. Each locks is atomic u32 which represents minimal frame number this transaction can see.
    readers: Arc<Semaphore>,
    /// Lock for single writer.
    writer: Mutex<()>,
    /// All transactions read or write must acquire this lock before they can do anything.
    /// When we want to run checkpoint we get write lock instead to block other transactions.
    /// While this can couse latency spikes, it prevents WAL from growing indefinitely.
    checkpoint_lock: RwLock<()>,
}

impl WriteAheadLog {
    pub fn open(
        db_file: &Arc<BlockIO<File>>,
        db_header: &Arc<MemDatabaseHeader>,
        page_cache: &Arc<ShardedClockCache>,
        wal_file_path: PathBuf,
    ) -> storage::Result<Self> {
        let file_exists = wal_file_path.exists();

        let mut file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(false)
            .truncate(false)
            .lock(true)
            .open(wal_file_path)?;

        if !file_exists {
            let buf = &mut [0; WAL_HEADER_SIZE];
            let default_header =
                WalHeader::default(db_header.page_size, db_header.get_database_size());
            default_header.write_to_buffer(buf);

            file.pwrite(0, buf)?;
            file.flush()?;
            file.sync()?;
        }

        let header = {
            let buf = &mut [0; WAL_HEADER_SIZE];
            file.pread(0, buf)?;
            let header = WalHeader::from_bytes(buf);

            // validate header checksum
            if header.checksum != header.checksum_self(None) {
                return Err(Error::InvalidChecksum);
            }
            header
        };

        let wal_file = Arc::new(BlockIO::new(
            file,
            db_header.page_size as usize,
            WAL_HEADER_SIZE,
        ));

        Self::new(wal_file, db_file.clone(), header, page_cache.clone())
    }

    pub fn new(
        wal_file: Arc<BlockIO<File>>,
        db_file: Arc<BlockIO<File>>,
        header: WalHeader,
        page_cache: Arc<ShardedClockCache>,
    ) -> storage::Result<Self> {
        let frame_size = header.page_size as usize + FRAME_HEADER_SIZE;
        // let backfilled_number = header.backfilled_number;
        // let checkpoint_seq_num = header.checkpoint_seq_num;
        let index = Arc::new(WalIndex::new());

        let checksum = header.checksum;

        let wal = Self {
            wal_file,
            db_file,
            frame_size,
            trigger_checkpoint: DEFAULT_CHECKPOINT_SIZE,
            header: Arc::new(MemWalHeader::from(header)),
            index,
            page_cache,
            // min_frame: AtomicU32::new(min_frame),
            max_frame: AtomicU32::new(0),
            last_checksum: AtomicU64::new(pack_u64(checksum.0, checksum.1)),
            // backfilled_number: AtomicU32::new(backfilled_number),
            // checkpoint_seq_num: AtomicU32::new(checkpoint_seq_num),
            // by default it is set to u32::MAX to indicate that it is free and it is quite not possible for WAL to outgrow 17 TB of 4KB pages.
            readers: Arc::new(Semaphore::new(READERS_NUM)),
            writer: Mutex::new(()),
            checkpoint_lock: RwLock::new(()),
        };

        wal.replay()?;

        Ok(wal)
    }

    /// Reconstructs all the changes registered in WAL and if needed performs
    /// checkpoint.
    pub fn replay(&self) -> storage::Result<()> {
        // block all other operations. needs exclusive access.
        let checkpoint_guard = self.checkpoint_lock.write();
        // raw WAL header.
        let header = self.header.into_raw_header();

        // size of each frame.
        let frame_size = header.page_size as usize + FRAME_HEADER_SIZE;

        // increases after each valid checkpoint.
        let mut max_frame = 0;
        let mut transaction_frames = vec![];

        // we start with wal header checksum as if WAL was empty.
        let mut running_checksum = header.checksum_self(None);
        let mut buffer = vec![0; frame_size];

        // start at the beginning of WAL.
        let mut frame_number = 1;

        loop {
            let (valid, is_commit, page_number) = validate_frame(
                &self.wal_file,
                frame_number,
                &mut running_checksum,
                &mut buffer,
            )?;
            if !valid {
                break;
            }

            transaction_frames.push((page_number, frame_number));

            if is_commit {
                for (pn, f) in transaction_frames.drain(..) {
                    self.index.insert_latest(pn, f);
                }
                max_frame = frame_number;
            }

            frame_number += 1;
        }

        self.set_max_frame(max_frame);
        self.set_last_checksum(running_checksum);

        drop(checkpoint_guard);

        self.checkpoint()?;

        Ok(())
    }

    pub fn write_header(&self) -> storage::Result<()> {
        let raw_header = self.header.into_raw_header();

        self.wal_file.write_header(&raw_header.to_bytes())?;

        Ok(())
    }

    pub fn get_min_frame(&self) -> FrameNumber {
        self.header.get_last_checkpointed()
    }

    pub fn get_max_frame(&self) -> FrameNumber {
        self.max_frame.load(Ordering::Acquire)
    }

    pub fn get_last_checksum(&self) -> Checksum {
        self.last_checksum.load_packed(Ordering::Acquire)
    }

    pub fn get_backfilled_number(&self) -> u32 {
        self.header.get_backfilled_number()
    }

    pub fn get_checkpoint_seq_num(&self) -> u32 {
        self.header.get_checkpoint_seq_num()
    }

    pub fn set_min_frame(&self, value: FrameNumber) {
        self.header.set_last_checkpointed(value);
    }

    pub fn set_max_frame(&self, value: FrameNumber) {
        self.max_frame.store(value, Ordering::Release);
    }

    pub fn set_last_checksum(&self, value: Checksum) {
        self.last_checksum
            .store_packed(value.0, value.1, Ordering::Release);
    }

    pub fn set_backfilled_number(&self, value: u32) {
        self.header.set_backfilled_number(value);
    }

    pub fn increment_checkpoint_seq_num(&self) {
        self.header.increment_checkpoint_seq_num();
    }
}

impl WriteAheadLog {
    pub fn begin_read_tx<'a>(&'a self) -> storage::Result<ReadTransaction<'a>> {
        ReadTransaction::begin(self)
    }

    pub fn begin_write_tx<'a>(&'a self) -> storage::Result<WriteTransaction<'a>> {
        WriteTransaction::begin(self)
    }

    pub fn commit(&self, mut tx: WriteTransaction) -> storage::Result<()> {
        let inner = unsafe { ManuallyDrop::take(&mut tx.inner) };
        std::mem::forget(tx);

        self.wal_file.persist()?;
        self.set_last_checksum(inner.last_checksum);
        self.set_max_frame(inner.max_frame);

        drop(inner.checkpoint_guard);

        self.checkpoint()?;

        Ok(())
    }

    fn calculate_frame_offset(&self, frame_number: FrameNumber) -> storage::Result<usize> {
        self.wal_file
            .calculate_offset(frame_number)
            .map_err(|err| err.into())
    }

    pub fn is_in_wal(&self, page_number: &PageNumber) -> bool {
        self.index.contains(page_number)
    }

    /// Reads only page from WAL (skips header).
    fn read_raw(&self, frame_number: FrameNumber, buffer: &mut [u8]) -> storage::Result<usize> {
        let offset = self.calculate_frame_offset(frame_number)? + FRAME_HEADER_SIZE;
        self.wal_file
            .raw_read(offset, buffer)
            .map_err(|err| err.into())
    }

    /// Reads given `page_number` from WAL. This operation doesn't interrupt
    /// other readers, because each reader is bounded to it's min and max frame.
    pub fn read_frame<Tx: ReadTx>(
        &self,
        transaction: &Tx,
        page_number: PageNumber,
        buffer_pool: &Arc<BufferPool>,
    ) -> storage::Result<Option<Page>> {
        // if given page number is not present in WAL we can simply return OK(None)
        if !self.is_in_wal(&page_number) {
            return Ok(None);
        }

        // pager::begin_read_page(&page)?;

        let visible_frame_number = self
            .index
            .get(&page_number, transaction.tx_max_frame())
            .ok_or(Error::PageNotFoundInWal(page_number))?;

        let mut buffer = buffer_pool.get();

        let read_result = self
            .wal_file
            .read(visible_frame_number, &mut buffer, FRAME_HEADER_SIZE);

        // complete_read_page(&read_result, &page, buffer);

        match read_result {
            Ok(bytes_read) => {
                if bytes_read == buffer.size() {
                    let offset = if page_number == 1 { WAL_HEADER_SIZE } else { 0 };
                    Ok(Some(Page::new(offset, buffer)))
                } else {
                    Err(Error::PageNotFoundInWal(page_number))
                }
            }
            Err(_) => Err(Error::PageNotFoundInWal(page_number)),
        }
    }

    pub fn append_frame<Tx: WriteTx>(
        &self,
        transaction: &mut Tx,
        page: ExclusivePageGuard,
        db_size: u32,
    ) -> storage::Result<()> {
        self.append_vectored(transaction, vec![page], db_size)
    }

    pub fn append_vectored<Tx: WriteTx>(
        &self,
        transaction: &mut Tx,
        mut pages: Vec<ExclusivePageGuard>,
        db_size: u32,
    ) -> storage::Result<()> {
        let pages_number = pages.len();

        let mut io_buffers = Vec::with_capacity(pages.len() * 2);
        let mut header_buffers: Vec<Vec<u8>> = (0..pages.len())
            .map(|_| vec![0; FRAME_HEADER_SIZE])
            .collect();

        let mut running_checksum = transaction.tx_last_checksum();

        for (i, page) in pages.iter().enumerate() {
            // begin_write_page(page)?;

            let page_number = page.id();
            let page_content = page.as_ptr();
            let checksum = checksum_bytes(page_content, Some(running_checksum));

            running_checksum = checksum;
            let commit_db_size = if i + 1 == pages.len() { db_size } else { 0 };

            let frame_header = FrameHeader {
                page_number,
                db_size: commit_db_size,
                checksum,
            };

            frame_header.to_bytes(&mut header_buffers[i][..]);
        }

        for (i, page) in pages.iter().enumerate() {
            io_buffers.push(IoSlice::new(&header_buffers[i]));
            io_buffers.push(page.as_io_slice());
        }

        let frame_number = transaction.tx_max_frame() + 1;

        let write_result = self.wal_file.write_vectored(frame_number, &mut io_buffers);

        for page in pages.iter_mut() {
            pager::complete_write_page(&write_result, page, false);
        }

        transaction.tx_set_last_checksum(running_checksum);
        transaction.tx_set_max_frame(transaction.tx_max_frame() + pages_number as FrameNumber);

        for (i, page) in pages.iter().enumerate() {
            self.index
                .insert(page.id(), frame_number + i as FrameNumber);
        }

        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        let max_frame = self.get_max_frame();
        // I don't think this is usefull for now
        let backfilled_number = self.get_backfilled_number() * 0;

        max_frame > self.trigger_checkpoint + backfilled_number
    }

    pub fn checkpoint(&self) -> storage::Result<()> {
        if !self.should_checkpoint() {
            return Ok(());
        }

        self._checkpoint()
    }

    pub fn force_checkpoint(&self) -> storage::Result<()> {
        self._checkpoint()
    }

    fn _checkpoint(&self) -> storage::Result<()> {
        let exclusive_lock = self.checkpoint_lock.write();
        // let mut wal_header = self.global_wal.header.lock();

        let max_frame = self.get_max_frame();

        let frames_to_checkpoint = self.index.latest_frames_sorted(max_frame);
        let to_backfill = frames_to_checkpoint.len() as u32;
        let mut temp_buffer = vec![0; self.frame_size - FRAME_HEADER_SIZE];

        // iterates over pages that should be moved to db file. If they are
        // present in cache and dirty, they are written straight from cache
        for (page_number, frame_number) in frames_to_checkpoint {
            let offset = self.db_file.calculate_offset(page_number)?;
            if let Some(mem_page) = self.page_cache.get(&page_number) {
                let guard = mem_page.lock_exclusive();
                if guard.is_dirty() {
                    self.db_file.raw_write(offset, guard.as_ptr())?;
                    guard.clear_dirty();
                }
            } else {
                self.read_raw(frame_number, &mut temp_buffer)?;
                self.db_file.raw_write(offset, &temp_buffer)?;
            }
        }
        // persist changes to db file
        self.db_file.persist()?;

        // change wal header params, because it will be truncated
        self.set_min_frame(0);
        self.set_max_frame(0);
        self.set_backfilled_number(to_backfill);
        self.increment_checkpoint_seq_num();

        self.header.update_checksum();

        // write header to WAL
        self.write_header()?;

        // persist changes to WAL header
        self.wal_file.persist()?;

        // removes all frames from WAL leaving the header
        self.wal_file.truncate_beginning(max_frame)?;

        self.index.clear();

        drop(exclusive_lock);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
