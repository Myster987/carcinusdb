use std::{cell::RefCell, ops::DerefMut, path::Path, rc::Rc, sync::Arc};

use arc_swap::ArcSwap;
use thiserror::Error;

use crate::{
    os::{Open, OpenOptions},
    sql::{self, schema::Catalog},
    storage::{
        self, PageNumber,
        btree::{BTreeCursor, BTreeType},
        buffer_pool::BufferPool,
        cache::ShardedClockCache,
        page::{DATABASE_HEADER_SIZE, DatabaseHeader},
        pager::Pager,
        wal::{
            WriteAheadLog,
            transaction::{ReadTransaction, ReadTx, WriteTransaction, WriteTx},
        },
    },
    tcp::server::{TcpServer, connection::Connection},
    utils::io::{BlockIO, IO},
    vm::{self, query_result::QueryResult},
};

pub const CARCINUSDB_MASTER_TABLE: &'static str = "carcinusdb_master";
pub const CARCINUSDB_MASTER_TABLE_ROOT: PageNumber = 1;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // io
    #[error("invalid hostname: {hostname}\nmessage: {msg}")]
    InvalidHostname { msg: String, hostname: String },
    #[error("provided path is not file: {0}")]
    InvalidFilePath(String),
    #[error("invalid port number: {0}")]
    InvalidPort(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    // env
    #[error(transparent)]
    Env(#[from] std::env::VarError),

    // internal
    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    #[error(transparent)]
    Storage(#[from] crate::storage::Error),

    #[error(transparent)]
    Sql(#[from] crate::sql::Error),

    #[error(transparent)]
    VmError(#[from] crate::vm::Error),

    // other
    #[error("error msg: {0}")]
    Other(String),
    #[error("unknown database error")]
    Unknown,
}

pub async fn run(hostname: String, port: u16) -> Result<()> {
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

pub fn handle_connection(conn: Connection) -> Result<()> {
    log::info!("Connection from: {}", conn.client_address());

    Ok(())
}

pub struct MemDatabaseHeader {
    inner: ArcSwap<DatabaseHeader>,
}

impl MemDatabaseHeader {
    pub fn new(db_header: DatabaseHeader) -> Self {
        Self {
            inner: ArcSwap::from_pointee(db_header),
        }
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<DatabaseHeader>> {
        self.inner.load()
    }

    pub fn load_full(&self) -> Arc<DatabaseHeader> {
        self.inner.load_full()
    }

    pub fn swap(&self, new: DatabaseHeader) -> Arc<DatabaseHeader> {
        self.inner.swap(Arc::new(new))
    }
}

pub struct Database {
    catalog: Arc<Catalog>,
    pager: Arc<Pager>,
}

impl Database {
    /// Calling this function will open existing db. In case that db file
    /// doesn't exist, it will create new one. Write Ahead Log (WAL) will
    /// also be initialized with "{path}-wal{.extension}" file name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// // this will create "test-db.db" and "test-db-wal.db"
    /// let db = Database::open("./test-db.db");
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let is_initialized = path.as_ref().exists();

        let file = OpenOptions::default()
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
        ));

        let wal_file_path = {
            let mut path_buf = path.as_ref().to_path_buf();

            let file_name = path_buf
                .file_prefix()
                .map(|s| s.to_str().unwrap())
                .expect("Expected to provide database file name.");

            let extension = path_buf
                .extension()
                .map(|s| format!(".{}", s.to_str().unwrap()))
                .unwrap_or("".to_string());

            let wal_file = format!("{file_name}-wal{extension}");

            path_buf.set_file_name(wal_file);

            path_buf
        };

        let buffer_pool = Arc::new(BufferPool::default(db_header.get_page_size()));

        let cache = Arc::new(ShardedClockCache::new(
            db_header.default_page_cache_size as usize,
        ));

        let db_header = Arc::new(MemDatabaseHeader::new(db_header));

        let wal = Arc::new(WriteAheadLog::open(
            &db_file,
            &db_header,
            &cache,
            wal_file_path,
            !db_header.load().is_consistent(),
        )?);

        let pager = Arc::new(Pager::new(
            db_header,
            db_file,
            buffer_pool,
            wal,
            cache,
            is_initialized,
        )?);

        let catalog = {
            let tx = pager.wal.begin_read_tx()?;
            let cursor = BTreeCursor::new(
                Rc::new(RefCell::new(tx)),
                &pager,
                CARCINUSDB_MASTER_TABLE_ROOT,
                1,
            );

            Arc::new(Catalog::from_cursor(cursor)?)
        };

        Ok(Self { pager, catalog })
    }

    pub fn begin_read<'a>(&'a self) -> Result<DatabaseReadTransaction> {
        let wal_tx = self.pager.wal.begin_read_tx()?;

        Ok(DatabaseReadTransaction {
            wal_tx: Rc::new(RefCell::new(wal_tx)),
            pager: self.pager.clone(),
            catalog: self.catalog.clone(),
        })
    }

    pub fn begin_write<'a>(&'a self) -> Result<DatabaseWriteTransaction<'a>> {
        let wal_tx = self.pager.wal.begin_write_tx()?;

        Ok(DatabaseWriteTransaction {
            wal_tx: Rc::new(RefCell::new(wal_tx)),
            pager: self.pager.clone(),
            catalog: self.catalog.clone(),
        })
    }
}

pub trait ReadDbTx {
    fn pager(&self) -> &Arc<Pager>;
    fn catalog(&self) -> &Arc<Catalog>;
    fn read_cursor<'tx>(&'tx self, root: PageNumber) -> BTreeCursor<'tx, impl ReadTx>;
}

pub trait WriteDbTx: ReadDbTx {
    fn write_cursor<'tx>(&'tx self, root: PageNumber) -> BTreeCursor<'tx, impl WriteTx>;
    fn create_btree(&self, btree_type: BTreeType) -> storage::Result<PageNumber>;
}

pub struct DatabaseReadTransaction {
    wal_tx: Rc<RefCell<ReadTransaction>>,
    pager: Arc<Pager>,
    catalog: Arc<Catalog>,
}

impl DatabaseReadTransaction {
    pub fn cursor(&self, root: PageNumber) -> BTreeCursor<'_, ReadTransaction> {
        BTreeCursor::new(self.wal_tx.clone(), &self.pager, root, 0)
    }

    pub fn execute<'tx>(&'tx self, sql: &str) -> Result<QueryResult<'tx>> {
        let statement = sql::pipeline(self, sql)?;

        let plan = vm::planner::plan_read(statement, self)?;
        vm::execute_read(plan).map_err(Into::into)
    }

    #[must_use]
    pub fn commit(self) -> Result<()> {
        Ok(())
    }
}

impl ReadDbTx for DatabaseReadTransaction {
    fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    fn read_cursor<'tx>(&'tx self, root: PageNumber) -> BTreeCursor<'tx, impl ReadTx> {
        self.cursor(root)
    }
}

pub struct DatabaseWriteTransaction<'tx> {
    wal_tx: Rc<RefCell<WriteTransaction<'tx>>>,
    pager: Arc<Pager>,
    catalog: Arc<Catalog>,
}

impl<'tx> DatabaseWriteTransaction<'tx> {
    const DEFAULT_BALANCE_PER_SIDE: u16 = 3;

    pub fn cursor(&self, root: PageNumber) -> BTreeCursor<'_, WriteTransaction<'tx>> {
        BTreeCursor::new(
            self.wal_tx.clone(),
            &self.pager,
            root,
            Self::DEFAULT_BALANCE_PER_SIDE,
        )
    }

    pub fn execute(&self, sql: &str) -> Result<QueryResult<'_>> {
        let statement = sql::pipeline(self, sql)?;

        let plan = vm::planner::plan_write(statement, self)?;
        vm::execute_write(self, plan).map_err(Into::into)
    }

    /// Flushes dirty pages into WAL and marks them as clean. Might run checkpoint.
    #[must_use]
    pub fn commit(self) -> Result<()> {
        let mut wal_tx = Rc::into_inner(self.wal_tx)
            .expect("Something is still using this transaction")
            .into_inner();

        self.pager.flush_dirty(&mut wal_tx, true)?;

        self.pager.wal.commit(&mut wal_tx)?;

        Ok(())
    }
}

impl<'a> ReadDbTx for DatabaseWriteTransaction<'a> {
    fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    fn read_cursor<'tx>(&'tx self, root: PageNumber) -> BTreeCursor<'tx, impl ReadTx> {
        self.cursor(root)
    }
}

impl<'tx> WriteDbTx for DatabaseWriteTransaction<'tx> {
    fn write_cursor<'a>(&'a self, root: PageNumber) -> BTreeCursor<'a, impl WriteTx> {
        self.cursor(root)
    }

    fn create_btree(&self, btree_type: BTreeType) -> storage::Result<PageNumber> {
        self.pager
            .btree_create(self.wal_tx.borrow_mut().deref_mut(), btree_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        sql::{
            record::RecordBuilder,
            types::{Value, text::Text},
        },
        storage::btree::{BTreeKey, DatabaseCursor, DeleteOptionsBuilder, InsertOptionsBuilder},
    };

    const KEYS_START: i64 = 1;
    const KEYS_END: i64 = 200_000;
    const TEST_DB_NAME: &'static str = "./test-db.db";

    #[test]
    fn test_insert() -> anyhow::Result<()> {
        simple_logger::init_with_level(log::Level::Debug)?;

        let db = Database::open(TEST_DB_NAME)?;

        let tx = db.begin_write()?;

        // balancing: 12.2 MB
        // dumb split: 22.9 MB

        {
            // scope cursor to drop before tx commit.
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            let start = KEYS_START;
            let end = KEYS_END;

            let options = InsertOptionsBuilder::new().build();

            for i in start..end {
                let mut record = RecordBuilder::new();

                record.add(Value::Null);
                record.add(Value::Int(i));
                record.add(Value::Text(Text::new(format!(
                    "Maciek Kowalski {i} i Antoni Kowalski {i}"
                ))));

                let record = record.serialize_to_record();

                cursor.insert(BTreeKey::new_table_key(i, Some(record)), options)?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_search() -> anyhow::Result<()> {
        simple_logger::init()?;

        let db = Database::open(TEST_DB_NAME)?;

        let tx = db.begin_read()?;

        {
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            for i in KEYS_START..KEYS_END {
                assert!(
                    cursor.seek(&BTreeKey::new_table_key(i, None))?.is_found(),
                    "Entry {} lost",
                    i
                );
                if i % 10_000 == 0 {
                    log::info!("Up to entry: {} B-tree is valid", i)
                }
            }

            log::info!("All keys present!");
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_linear_scan() -> anyhow::Result<()> {
        simple_logger::init_with_level(log::Level::Debug)?;

        let db = Database::open(TEST_DB_NAME)?;

        let tx = db.begin_read()?;

        {
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            let mut count = 0;

            while cursor.next()? {
                count += 1;
            }

            println!("min entry:\n{:?}", cursor.min());
            println!("max entry:\n{:?}", cursor.max());

            log::info!("Scaned entries: {}", count);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_delete() -> anyhow::Result<()> {
        simple_logger::init()?;

        let db = Database::open(TEST_DB_NAME)?;

        let tx = db.begin_write()?;

        {
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            let options = DeleteOptionsBuilder::new().returning().build();

            let deleted_record = cursor.delete(&BTreeKey::new_table_key(4, None), options)?;

            println!("{:?}", deleted_record);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_execute() -> anyhow::Result<()> {
        simple_logger::init()?;

        let db = Database::open(TEST_DB_NAME)?;

        let tx = db.begin_write()?;

        {
            let query = tx.execute("CREATE TABLE test (id INT PRIMARY KEY, name TEXT);")?;

            log::debug!("Query: {:?}", query);

            for i in 1..=10 {
                let sql = format!("INSERT INTO test VALUES ({i}, 'test_{i}');");
                let query = tx.execute(&sql)?;
                println!("{}", query.to_string()?);
            }

            let query = tx.execute("UPDATE test SET name = 'macie' WHERE id = 1;")?;
            println!("{}", query.to_string()?);

            let query = tx.execute("SELECT * FROM test;")?;

            println!("{}", query.to_string()?);

            let query = tx.execute("DELETE FROM test WHERE id >= 5;")?;

            println!("{}", query.to_string()?);

            let query = tx.execute("SELECT * FROM test;")?;

            println!("{}", query.to_string()?);
        }

        tx.commit()?;

        Ok(())
    }
}
