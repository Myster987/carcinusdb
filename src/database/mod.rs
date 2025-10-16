use std::{path::PathBuf, sync::Arc};

use crate::{
    error::DatabaseResult,
    storage::{
        buffer_pool::GlobalBufferPool, cache::ShardedLruCache, pager::Pager, wal::WalManager,
    },
    tcp::server::{TcpServer, connection::Connection},
};

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

#[derive(Debug)]
pub struct DatabaseConfig {
    db_path_path: PathBuf,
    page_size: usize,
    cache_capacity: usize,
    pool_init_size: usize,
}

pub struct Database {
    config: DatabaseConfig,
    global_pool: GlobalBufferPool,
    wal_manager: WalManager,
    cache: Arc<ShardedLruCache>,
}

impl Database {
    pub fn new(db_file_path: PathBuf) -> Self {
        todo!()
        // let global_pool = GlobalBufferPool::default(config.page_size, config.pool_init_size);
        // let wal_manager = WalManager::new(wal_file_path, db_file, config.page_size);
        // let cache = Arc::new(ShardedLruCache::new(config.cache_capacity));

        // Self {
        //     config,
        //     global_pool,
        //     wal_manager,
        //     cache,
        // }
    }

    pub fn pager(&self) -> Pager {
        todo!()
    }
}
