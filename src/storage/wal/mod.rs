use std::{
    collections::VecDeque,
    fs::File,
    io::IoSlice,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::{
    os::{Open, OpenOptions},
    storage::{
        Error, FrameNumber, PageNumber, StorageResult,
        buffer_pool::LocalBufferPool,
        page::MAX_PAGE_SIZE,
        pager::{self, MemPageRef, Pager},
        wal::locks::{PackedU64, ReadGuard, ReadersPool, WriteGuard},
    },
    utils::{
        self,
        buffer::Buffer,
        bytes::{byte_swap_u32, pack_u64},
        io::BlockIO,
    },
};

mod locks;

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
        let checksum = (
            u32::from_le_bytes(buffer[20..24].try_into().unwrap()),
            u32::from_le_bytes(buffer[24..28].try_into().unwrap()),
        );

        Self {
            page_size,
            checkpoint_seq_num,
            last_checkpointed,
            db_size,
            backfilled_number,
            checksum,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
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
        buffer[20..24].copy_from_slice(&self.checksum.0.to_le_bytes());
        buffer[24..28].copy_from_slice(&self.checksum.1.to_le_bytes());
    }

    fn checksum_self(&self, seed: Option<Checksum>) -> Checksum {
        let header_bytes = &utils::cast::bytes_of(self)[..WAL_HEADER_SIZE_NO_CHECKSUM];
        checksum_bytes(header_bytes, seed)
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

    pub fn to_bytes(self, buffer: &mut [u8]) {
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
    ) -> StorageResult<Self> {
        // If WAL doesn't exist, then it creates header with default parameters
        let mut header = (!wal_file_path.exists()).then(|| {
            WalHeader::default(
                page_size,
                db_file
                    .size_in_blocks()
                    .expect("Database file size must be known") as u32,
            )
        });

        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .open(wal_file_path)?;

        let wal_file = Arc::new(BlockIO::new(file, page_size as usize, WAL_HEADER_SIZE));

        if header.is_none() {
            let buf = &mut [0; WAL_HEADER_SIZE];
            wal_file.read_header(buf)?;
            let h = WalHeader::from_bytes(buf);

            // validate header checksum
            if h.checksum != h.checksum_self(None) {
                return Err(Error::InvalidChecksum);
            }

            header = Some(h);
        }

        let global_wal = GlobalWal::new(wal_file, db_file, header.unwrap())?;

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
    header: Arc<Mutex<WalHeader>>,
    /// WAL index that sppeds up searching for frame.
    index: Arc<WalIndex>,
    /// Last checkpointed entry in WAL.
    min_frame: AtomicU32,
    /// Last commited frame in transaction.
    max_frame: AtomicU32,
    /// Checksum of last frame in WAL. It is cumulative checksum of all pages. Stored as two u32 packed in single atomic.
    last_checksum: AtomicU64,

    /// Number of frames transfered to DB from WAL.
    backfilled_number: AtomicU32,
    /// Counter of checkpoints
    checkpoint_seq_num: AtomicU32,

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
    ) -> StorageResult<Self> {
        let frame_size = header.page_size as usize + FRAME_HEADER_SIZE;
        let backfilled_number = header.backfilled_number;
        let checkpoint_seq_num = header.checkpoint_seq_num;
        let index = Arc::new(WalIndex::new());

        let min_frame = header.last_checkpointed;
        let mut max_frame = 0;
        let mut transaction_frames = vec![];

        let mut running_checksum = header.checksum_self(None);
        let mut buffer = vec![0; frame_size];

        for frame_number in 0..u32::MAX {
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
            header: Arc::new(Mutex::new(header)),
            index,
            min_frame: AtomicU32::new(min_frame),
            max_frame: AtomicU32::new(max_frame),
            last_checksum: AtomicU64::new(pack_u64(running_checksum.0, running_checksum.1)),
            backfilled_number: AtomicU32::new(backfilled_number),
            checkpoint_seq_num: AtomicU32::new(checkpoint_seq_num),
            // by default it is set to u32::MAX to indicate that it is free and it is quite not possible for WAL to outgrow 17 TB of 4KB pages.
            readers: Arc::new(ReadersPool::new()),
            writer: Mutex::new(()),
            checkpoint_lock: RwLock::new(()),
        })
    }

    pub fn get_min_frame(&self) -> FrameNumber {
        self.min_frame.load(Ordering::Acquire)
    }

    pub fn get_max_frame(&self) -> FrameNumber {
        self.max_frame.load(Ordering::Acquire)
    }

    pub fn get_last_checksum(&self) -> Checksum {
        self.last_checksum.load_packed(Ordering::Acquire)
    }

    pub fn get_backfilled_number(&self) -> u32 {
        self.backfilled_number.load(Ordering::Acquire)
    }

    pub fn get_checkpoint_seq_num(&self) -> u32 {
        self.checkpoint_seq_num.load(Ordering::Acquire)
    }

    pub fn set_min_frame(&self, value: FrameNumber) {
        self.min_frame.store(value, Ordering::Release);
    }

    pub fn set_max_frame(&self, value: FrameNumber) {
        self.max_frame.store(value, Ordering::Release);
    }

    pub fn set_last_checksum(&self, value: Checksum) {
        self.last_checksum
            .store_packed(value.0, value.1, Ordering::Release);
    }

    pub fn set_backfilled_number(&self, value: u32) {
        self.backfilled_number.store(value, Ordering::Release);
    }

    pub fn increment_checkpoint_seq_num(&self) {
        self.checkpoint_seq_num.fetch_add(1, Ordering::Release);
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

    pub fn is_in_wal(&self, page_number: &PageNumber) -> bool {
        self.global_wal.index.contains(page_number)
    }

    pub fn begin_read_tx(&mut self) -> StorageResult<ReadGuard<READERS_NUM>> {
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

    pub fn begin_write_tx(&mut self) -> StorageResult<WriteGuard> {
        let checkpoint_guard = self.global_wal.checkpoint_lock.read();
        let mutex_guard = self.global_wal.writer.lock();

        let guard = WriteGuard::new(checkpoint_guard, mutex_guard);

        self.min_frame = self.global_wal.get_min_frame();
        self.max_frame = self.global_wal.get_max_frame();
        self.last_checksum = self.global_wal.get_last_checksum();

        Ok(guard)
    }

    pub fn end_read_tx(&mut self, guard: ReadGuard<READERS_NUM>) -> StorageResult<()> {
        self.min_frame = 0;
        self.max_frame = 0;

        drop(guard);

        Ok(())
    }

    /// Ends write transaction by dropping write locks and updates all
    pub fn end_write_tx(&mut self, guard: WriteGuard) -> StorageResult<()> {
        self.global_wal.wal_file.flush()?;
        self.global_wal.wal_file.sync()?;

        self.global_wal.set_last_checksum(self.last_checksum);

        self.global_wal.set_max_frame(self.max_frame);

        self.min_frame = 0;
        self.max_frame = 0;

        drop(guard);

        Ok(())
    }

    /// Reads only page from WAL (skips header).
    fn read_raw(&self, frame_number: FrameNumber, buffer: &mut [u8]) -> std::io::Result<usize> {
        let offset = WAL_HEADER_SIZE
            + (self.global_wal.frame_size * frame_number as usize)
            + FRAME_HEADER_SIZE;
        self.global_wal.wal_file.raw_read(offset, buffer)
    }

    /// Reads given `page_number` from WAL. This operation doesn't interrupt
    /// other readers, because each reader is bounded to it's min and max frame.
    pub fn read_frame(
        &mut self,
        page_number: PageNumber,
        page: &MemPageRef,
        buffer: &mut Buffer,
    ) -> StorageResult<Option<MemPageRef>> {
        // if given page number is not present in WAL we can simply return OK(None)
        if !self.is_in_wal(&page_number) {
            return Ok(None);
        }

        pager::begin_read_page(&page)?;

        let visible_frame_number = self
            .global_wal
            .index
            .get(&page_number, self.max_frame)
            .ok_or(Error::PageNotFoundInWal(page_number))?;

        let read_result = self.read_raw(visible_frame_number, &mut buffer[..]);

        pager::complete_read_page(read_result, page, buffer).map(|pg| Some(pg))
    }

    /// Writes content of `self.temp_buffer` into WAL at given `frame_number`
    fn write_raw(&self, frame_number: FrameNumber, buffer: &[u8]) -> std::io::Result<usize> {
        self.global_wal.wal_file.write(frame_number, buffer)
    }

    pub fn append_frame(&mut self, page: MemPageRef, db_size: u32) -> StorageResult<MemPageRef> {
        self.append_vectored(vec![page], db_size)
            .map(|mut res| res.pop().unwrap())
    }

    pub fn append_vectored(
        &mut self,
        pages: Vec<MemPageRef>,
        db_size: u32,
    ) -> StorageResult<Vec<MemPageRef>> {
        let mut io_buffers = Vec::with_capacity(pages.len() * 2);
        let mut header_buffers: Vec<Vec<u8>> = (0..pages.len())
            .map(|_| vec![0; FRAME_HEADER_SIZE])
            .collect();

        let mut running_checksum = self.last_checksum;

        for (i, page) in pages.iter().enumerate() {
            let page_number = page.get().id;
            let page_content = &page.get_content().as_ptr();
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
            io_buffers.push(page.get_content().as_io_slice());
        }

        let frame_number = self.max_frame + 1;

        self.global_wal
            .wal_file
            .write_vectored(frame_number, &mut io_buffers)?;

        self.last_checksum = running_checksum;
        self.max_frame = self.max_frame + pages.len() as FrameNumber;

        for (i, page) in pages.iter().enumerate() {
            self.global_wal
                .index
                .insert(page.get().id, frame_number + i as FrameNumber);
        }

        Ok(pages)
    }

    fn should_checkpoint(&self) -> bool {
        let max_frame = self.global_wal.get_max_frame();
        let backfilled_number = self.global_wal.get_backfilled_number();

        max_frame > self.global_wal.checkpoint_size + backfilled_number
    }

    pub fn checkpoint(&mut self, pager: &mut Pager) -> StorageResult<()> {
        if !self.should_checkpoint() {
            return Ok(());
        }

        let exclusive_lock = self.global_wal.checkpoint_lock.write();
        let mut wal_header = self.global_wal.header.lock();

        let max_frame = self.global_wal.get_max_frame();

        let frames_to_checkpoint = self.global_wal.index.latest_frames_sorted(max_frame);
        let to_backfill = frames_to_checkpoint.len() as u32;
        let mut temp_buffer = vec![0; self.global_wal.frame_size - FRAME_HEADER_SIZE];

        for (page_number, frame_number) in frames_to_checkpoint {
            if let Some(mem_page) = pager.page_cache.get(&page_number) {
                if mem_page.is_dirty() {
                    pager.write_page(mem_page)?;
                }
            } else {
                self.read_raw(frame_number, &mut temp_buffer)?;
                pager.write_raw(page_number, &temp_buffer)?;
            }
        }

        self.global_wal.db_file.flush()?;
        self.global_wal.db_file.sync()?;

        wal_header.backfilled_number = to_backfill;
        wal_header.checkpoint_seq_num = wal_header.checkpoint_seq_num.wrapping_add(1);

        // wal_header.db_size = self.global_wal.db_file.s
        // self.global_wal.set
        // self.global_wal.set_backfilled_number(to_backfill as u32);
        // self.global_wal.increment_checkpoint_seq_num();

        // removes all frames from WAL leaving the header
        self.global_wal.wal_file.truncate_beginning(max_frame)?;
        // if min_active_frame == self.global_wal.get_min_frame()

        todo!()
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
) -> StorageResult<(bool, bool, PageNumber)> {
    if wal_file.read(frame_number, buffer).is_err() {
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

#[cfg(test)]
mod tests {
    use super::*;
}
