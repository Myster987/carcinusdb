use std::{
    collections::VecDeque,
    fs::File,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU32, Arc},
};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{
    os::{Open, OpenOptions},
    storage::{FrameNumber, PageNumber, StorageResult, page::MAX_PAGE_SIZE},
    utils::{self, bytes::byte_swap_u32, io::BlockIO},
};

const WAL_HEADER_SIZE: usize = size_of::<WalHeader>();
const WAL_HEADER_SIZE_NO_CHECKSUM: usize = WAL_HEADER_SIZE - size_of::<[u32; 2]>();
const FRAME_HEADER_SIZE: usize = size_of::<FrameHeader>();
const READERS_NUM: usize = 5;

#[repr(C)]
#[derive(Debug)]
pub struct WalHeader {
    /// Size of invidual page in wal (not frame)
    page_size: u16,
    /// Incremented with each checkpoint
    checkpoint_seq_num: u32,
    /// Last checkpointed entry in WAL (if WAL size is 0, then it is set to u32::MAX)
    last_checkpointed: FrameNumber,
    /// Checksum of header
    checksum: [u32; 2],
}

impl WalHeader {
    /// Recalculates checksum of all fields except `checksum`.
    pub fn update_checksum(&mut self) {
        let header_bytes = &utils::cast::bytes_of(self)[..WAL_HEADER_SIZE_NO_CHECKSUM];

        self.checksum = checksum_bytes(header_bytes, None);
    }

    pub fn default(page_size: u16) -> Self {
        let mut wal_hedaer = Self {
            page_size,
            checkpoint_seq_num: 0,
            last_checkpointed: u32::MAX,
            checksum: [0; 2],
        };
        wal_hedaer.update_checksum();
        wal_hedaer
    }

    pub fn from_bytes(buffer: &[u8]) -> Self {
        let page_size = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        let checkpoint_seq_num = u32::from_le_bytes(buffer[2..6].try_into().unwrap());
        let last_checkpointed = u32::from_le_bytes(buffer[6..10].try_into().unwrap());
        let checksum = [
            u32::from_le_bytes(buffer[10..14].try_into().unwrap()),
            u32::from_le_bytes(buffer[14..18].try_into().unwrap()),
        ];

        Self {
            page_size,
            checkpoint_seq_num,
            last_checkpointed,
            checksum,
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
    /// Checksum of header (copied from `WalHeader`)
    checksum_header: u32,
    /// Checksum of page
    checksum_page: u32,
}

pub struct WalManager {
    global_wal: Arc<GlobalWal>,
}

impl WalManager {
    pub fn new(
        wal_file_path: PathBuf,
        db_file: Arc<BlockIO<File>>,
        page_size: u16,
    ) -> StorageResult<Self> {
        let mut header = if wal_file_path.exists() {
            None
        } else {
            Some(WalHeader::default(page_size))
        };
        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .open(wal_file_path)?;
        let wal_file = Arc::new(BlockIO::new(file, page_size as usize, WAL_HEADER_SIZE));

        if header.is_none() {
            let buf = &mut [0; WAL_HEADER_SIZE];
            wal_file.read_header(buf)?;
            header = Some(WalHeader::from_bytes(buf));
        }

        let global_wal = GlobalWal::new(wal_file, db_file, header.unwrap());

        Ok(Self {
            global_wal: Arc::new(global_wal),
        })
    }
}

pub struct GlobalWal {
    wal_file: Arc<BlockIO<File>>,
    db_file: Arc<BlockIO<File>>,

    header: Arc<Mutex<WalHeader>>,
    index: Arc<WalIndex>,

    readers: [AtomicU32; READERS_NUM]
}

impl GlobalWal {
    pub fn new(
        wal_file: Arc<BlockIO<File>>,
        db_file: Arc<BlockIO<File>>,
        header: WalHeader,
    ) -> Self {
        Self {
            wal_file,
            db_file,
            header: Arc::new(Mutex::new(header)),
            index: Arc::new(WalIndex::new()),
        }
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
}

pub fn checksum_bytes(data: &[u8], initial_checksum: Option<[u32; 2]>) -> [u32; 2] {
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
