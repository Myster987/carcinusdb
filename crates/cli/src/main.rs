use std::env;

#[tokio::main]
async fn main() -> Result<(), carcinusdb::CarcinusdbError> {
    use carcinusdb::tcp::client::ClientConnection;

    let port = env::args()
        .nth(1)
        .unwrap_or("4000".to_string())
        .parse::<u16>()
        .expect("Expected port number.");

    let mut client = ClientConnection::connect(&format!("127.0.0.1:{port}")).await?;

    println!("Connected to database at port {port}. In order to exit, write \".quit\"");
    println!("Query table \"carcinusdb_master\" in order to see schema definition.");

    loop {
        let mut sql = String::new();

        while !sql.contains(";") && sql.trim() != ".quit" {
            carcinusdb::utils::io::input_buffered("carcinsudb> ", &mut sql)?;
        }

        let sql = sql.trim();

        if sql == ".quit" {
            break;
        }

        let query_result = client.query(sql).await?;

        println!("{query_result}");
    }

    Ok(())
}
