use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    error::DatabaseResult,
    storage::{buffer_pool::GlobalBufferPool, cache::LruPageCache},
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
    page_size: usize,
    cache_capacity: usize,
    pool_init_size: usize,
}

#[derive(Debug)]
pub struct Database {
    global_pool: GlobalBufferPool,
    cache: Arc<Mutex<LruPageCache>>,
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Self {
        let global_pool = GlobalBufferPool::default(config.page_size, config.pool_init_size);
        Self {
            global_pool,
            cache: Arc::new(Mutex::new(LruPageCache::new(config.cache_capacity))),
        }
    }
}
