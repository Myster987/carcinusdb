use std::{
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use crate::{
    error::{DatabaseError, DatabaseResult},
    os::{Open, OpenOptions},
    storage::{
        PageNumber,
        btree::BTreeCursor,
        buffer_pool::BufferPool,
        cache::ShardedClockCache,
        page::{DATABASE_HEADER_SIZE, DatabaseHeader, Page},
        pager::Pager,
        wal::{
            WriteAheadLog,
            transaction::{ReadTransaction, WriteTransaction},
        },
    },
    tcp::server::{TcpServer, connection::Connection},
    utils::io::{BlockIO, FileOps, IO},
};

const CARCINUSDB_MASTER_TABLE: &'static str = "carcinusdb_master";

pub async fn run(hostname: String, port: u16) -> DatabaseResult<()> {
    log::info!("Starting CarcinusDB...");

    let addr = format!("{hostname}:{port}")
        .parse()
        .expect("Invalid hostname or port.");

    let tcp_server = TcpServer::new(addr).await?;
    log::info!("Listening on: {}", tcp_server.local_addr());

    loop {
        let conn = tcp_server.accept_connection().await?;
        tokio::spawn(async move { handle_connection(conn) });
    }

    Ok(())
}

pub fn handle_connection(conn: Connection) -> DatabaseResult<()> {
    log::info!("Connection from: {}", conn.client_address());

    Ok(())
}

pub struct MemDatabaseHeader {
    pub version: u32,
    pub page_size: u32,
    pub reserved_space: u16,
    change_counter: AtomicU32,
    database_size: AtomicU32,
    first_freelist_page: AtomicU32,
    freelist_pages: AtomicU32,
    pub default_page_cache_size: u32,
}

impl MemDatabaseHeader {
    pub fn into_raw_header(&self) -> DatabaseHeader {
        DatabaseHeader {
            version: self.version,
            page_size: self.page_size,
            reserved_space: self.reserved_space,
            change_counter: self.get_change_counter(),
            database_size: self.get_database_size(),
            first_freelist_page: self.get_first_freelist_page(),
            freelist_pages: self.get_freelist_pages(),
            default_page_cache_size: self.default_page_cache_size,
        }
    }

    pub fn get_change_counter(&self) -> u32 {
        self.change_counter.load(Ordering::Acquire)
    }

    pub fn get_database_size(&self) -> u32 {
        self.database_size.load(Ordering::Acquire)
    }

    pub fn get_first_freelist_page(&self) -> PageNumber {
        self.first_freelist_page.load(Ordering::Acquire)
    }

    pub fn get_freelist_pages(&self) -> PageNumber {
        self.freelist_pages.load(Ordering::Acquire)
    }

    pub fn increment_change_counter(&self) {
        self.change_counter.fetch_add(1, Ordering::Release);
    }

    /// Adds to current db size. Returns previous value.
    pub fn add_database_size(&self, add: u32) -> u32 {
        self.database_size.fetch_add(add, Ordering::Release)
    }

    pub fn set_first_freelist_page(&self, page_number: PageNumber) {
        self.first_freelist_page
            .store(page_number, Ordering::Release);
    }

    pub fn sub_freelist_pages(&self, sub: u32) {
        let _ = self
            .freelist_pages
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |val| {
                Some(val.saturating_sub(sub))
            });
    }
}

impl From<DatabaseHeader> for MemDatabaseHeader {
    fn from(db_header: DatabaseHeader) -> Self {
        Self {
            version: db_header.version,
            page_size: db_header.page_size,
            reserved_space: db_header.reserved_space,
            change_counter: AtomicU32::new(db_header.change_counter),
            database_size: AtomicU32::new(db_header.database_size),
            first_freelist_page: AtomicU32::new(db_header.first_freelist_page),
            freelist_pages: AtomicU32::new(db_header.freelist_pages),
            default_page_cache_size: db_header.default_page_cache_size,
        }
    }
}

pub struct Database {
    pager: Arc<Pager>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> DatabaseResult<Self> {
        let is_initialized = path.as_ref().exists();

        let mut file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .sync_on_write(false)
            .lock(true)
            .open(&path)?;

        let db_header = if is_initialized {
            let buf = &mut [0; DATABASE_HEADER_SIZE];
            file.pread(0, buf)?;
            DatabaseHeader::from_bytes(buf)
        } else {
            DatabaseHeader::default()
        };

        let db_file = Arc::new(BlockIO::new(
            file,
            db_header.get_page_size(),
            DATABASE_HEADER_SIZE,
            false,
        ));

        let wal_file_path = {
            let mut path = path.as_ref().to_path_buf();

            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                let new_file_name = format!("{}-wal", file_name);
                path.set_file_name(new_file_name);
            } else {
                panic!("Expected to include database file name in path: {:?}", path);
            }

            path
        };

        let buffer_pool = Arc::new(BufferPool::default(db_header.get_page_size()));

        let cache = Arc::new(ShardedClockCache::new(
            db_header.default_page_cache_size as usize,
        ));

        let db_header = Arc::new(MemDatabaseHeader::from(db_header));

        let wal = Arc::new(WriteAheadLog::open(
            &db_file,
            &db_header,
            &cache,
            wal_file_path,
        )?);

        let pager = Arc::new(Pager::new(
            db_header,
            db_file,
            buffer_pool,
            wal,
            cache,
            is_initialized,
        )?);

        Ok(Self { pager })
    }

    pub fn begin_read<'a>(&'a self) -> DatabaseResult<DatabaseReadTransaction<'a>> {
        let wal_tx = self.pager.wal.begin_read_tx()?;

        Ok(DatabaseReadTransaction {
            wal_tx,
            pager: self.pager.clone(),
        })
    }

    pub fn begin_write<'a>(&'a self) -> DatabaseResult<DatabaseWriteTransaction<'a>> {
        let wal_tx = self.pager.wal.begin_write_tx()?;

        Ok(DatabaseWriteTransaction {
            wal_tx,
            pager: self.pager.clone(),
        })
    }
}

pub struct DatabaseReadTransaction<'tx> {
    wal_tx: ReadTransaction<'tx>,
    pager: Arc<Pager>,
}

impl<'tx> DatabaseReadTransaction<'tx> {
    pub fn cursor(&'tx self, root: PageNumber) -> BTreeCursor<'tx, ReadTransaction<'tx>> {
        BTreeCursor::new(&self.wal_tx, &self.pager, root)
    }

    pub fn commit(self) -> DatabaseResult<()> {
        Ok(())
    }
}

pub struct DatabaseWriteTransaction<'tx> {
    wal_tx: WriteTransaction<'tx>,
    pager: Arc<Pager>,
}

impl<'tx> DatabaseWriteTransaction<'tx> {
    pub fn cursor(&'tx self, root: PageNumber) -> BTreeCursor<'tx, WriteTransaction<'tx>> {
        BTreeCursor::new(&self.wal_tx, &self.pager, root)
    }

    pub fn commit(self) -> DatabaseResult<()> {
        self.pager.wal.commit(self.wal_tx)?;

        Ok(())
    }
}
