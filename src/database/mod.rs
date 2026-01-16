use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::{
        PageNumber,
        buffer_pool::GlobalBufferPool,
        cache::ShardedLruCache,
        page::{DATABASE_HEADER_SIZE, DatabaseHeader},
        pager::Pager,
        wal::WalManager,
    },
    tcp::server::{TcpServer, connection::Connection},
    utils::io::{BlockIO, IO},
};

pub const GLOBAL_INIT_POOL_SIZE: usize = 250;
pub const LOCAL_INIT_POOL_SIZE: usize = 40;

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

    pub fn add_database_size(&self, add: u32) {
        self.database_size.fetch_add(add, Ordering::Release);
    }

    pub fn set_first_freelist_page(&self, page_number: PageNumber) {
        self.first_freelist_page
            .store(page_number, Ordering::Release);
    }

    pub fn sub_freelist_pages(&self, sub: u32) {
        self.freelist_pages
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
    header: Arc<MemDatabaseHeader>,
    db_file: Arc<BlockIO<File>>,

    global_pool: GlobalBufferPool,
    wal_manager: WalManager,
    cache: Arc<ShardedLruCache>,
}

impl Database {
    pub fn new(db_file_path: PathBuf) -> DatabaseResult<Self> {
        let file_exists = db_file_path.exists();

        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .bypass_cache(true)
            .truncate(false)
            .sync_on_write(false)
            .lock(true)
            .open(db_file_path.clone())?;

        if !file_exists {
            let buf = &mut [0; DATABASE_HEADER_SIZE];
            let default_header = DatabaseHeader::default();
            default_header.write_to_buffer(buf);

            file.pwrite(0, buf)?;
        }

        let db_header = {
            let buf = &mut [0; DATABASE_HEADER_SIZE];
            file.pread(0, buf)?;

            DatabaseHeader::from_bytes(buf)
        };

        let db_file = Arc::new(BlockIO::new(
            file,
            db_header.get_page_size(),
            DATABASE_HEADER_SIZE,
        ));

        let wal_file_path = {
            let mut path = db_file_path;

            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                let new_file_name = format!("{}-wal", file_name);
                path.set_file_name(new_file_name);
            } else {
                panic!("Expected to include database file name in path: {:?}", path);
            }

            path
        };

        let global_pool =
            GlobalBufferPool::default(db_header.get_page_size(), GLOBAL_INIT_POOL_SIZE);

        let cache = Arc::new(ShardedLruCache::new(
            db_header.default_page_cache_size as usize,
        ));

        let wal_manager = WalManager::new(
            wal_file_path,
            db_file.clone(),
            db_header.get_page_size() as u32,
            db_header.database_size,
            cache.clone(),
        )?;

        Ok(Self {
            header: Arc::new(MemDatabaseHeader::from(db_header)),
            db_file,
            global_pool,
            wal_manager,
            cache,
        })
    }

    fn init(path: &Path) -> DatabaseResult<()> {
        todo!()
    }

    pub fn pager(&self) -> Pager {
        Pager::new(
            self.header.clone(),
            self.db_file.clone(),
            self.global_pool.local_pool(LOCAL_INIT_POOL_SIZE),
            self.wal_manager.local_wal(),
            self.cache.clone(),
        )
    }
}
