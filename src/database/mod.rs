use std::{fs::File, path::PathBuf, sync::Arc};

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::{
        buffer_pool::GlobalBufferPool,
        cache::ShardedLruCache,
        page::{DATABASE_HEADER_SIZE, DEFAULT_PAGE_SIZE, DatabaseHeader},
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

pub struct Database {
    header: DatabaseHeader,
    db_file: Arc<BlockIO<File>>,
    global_pool: GlobalBufferPool,
    wal_manager: WalManager,
    cache: Arc<ShardedLruCache>,
}

impl Database {
    pub fn new(db_file_path: PathBuf) -> DatabaseResult<Self> {
        let mut header = (!db_file_path.exists()).then(|| DatabaseHeader::default());

        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .open(db_file_path.clone())?;

        if header.is_none() {
            let buf = &mut [0; DATABASE_HEADER_SIZE];
            file.pread(0, buf)?;
            let h = DatabaseHeader::from_bytes(buf);

            header = Some(h);
        }

        let db_header = header.unwrap();
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
        let wal_manager = WalManager::new(
            wal_file_path,
            db_file.clone(),
            db_header.get_page_size() as u32,
        )?;
        let cache = Arc::new(ShardedLruCache::new(
            db_header.default_page_cache_size as usize,
        ));

        Ok(Self {
            header: db_header,
            db_file,
            global_pool,
            wal_manager,
            cache,
        })
    }

    pub fn pager(&self) -> Pager {
        Pager::new(
            self.db_file.clone(),
            self.global_pool.local_pool(LOCAL_INIT_POOL_SIZE),
            self.wal_manager.local_wal(),
            self.cache.clone(),
        )
    }
}
