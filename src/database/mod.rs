use crate::{
    error::DatabaseResult,
    storage::file_system_manager::FILE_SYSTEM_MANAGER,
    tcp::server::{TcpServer, connection::Connection},
};

pub mod config;
pub mod metadata;

pub async fn run(hostname: String, port: u16) -> DatabaseResult<()> {
    log::info!("Starting CarcinusDB...");

    config::create_base_dir()?;

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
pub struct Database {}

impl Database {}
