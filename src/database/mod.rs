use std::{cell::RefCell, path::Path, rc::Rc, sync::Arc};

use arc_swap::ArcSwap;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::{
        PageNumber,
        btree::BTreeCursor,
        buffer_pool::BufferPool,
        cache::ShardedClockCache,
        page::{DATABASE_HEADER_SIZE, DatabaseHeader},
        pager::Pager,
        wal::{
            WriteAheadLog,
            transaction::{ReadTransaction, WriteTransaction},
        },
    },
    tcp::server::{TcpServer, connection::Connection},
    utils::io::{BlockIO, IO},
};

pub const CARCINUSDB_MASTER_TABLE: &'static str = "carcinusdb_master";
pub const CARCINUSDB_MASTER_TABLE_ROOT: PageNumber = 1;

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

// pub struct MemDatabaseHeader {
//     pub version: u32,
//     pub page_size: u32,
//     pub reserved_space: u16,
//     change_counter: AtomicU32,
//     database_size: AtomicU32,
//     first_freelist_page: AtomicU32,
//     freelist_pages: AtomicU32,
//     pub default_page_cache_size: u32,
//     version_valid_for: AtomicU32,
// }

// impl MemDatabaseHeader {
//     pub fn into_raw_header(&self) -> DatabaseHeader {
//         DatabaseHeader {
//             version: self.version,
//             page_size: self.page_size,
//             reserved_space: self.reserved_space,
//             change_counter: self.get_change_counter(),
//             database_size: self.get_database_size(),
//             first_freelist_page: self.get_first_freelist_page(),
//             freelist_pages: self.get_freelist_pages(),
//             default_page_cache_size: self.default_page_cache_size,
//             version_valid_for: self.get_version_valid_for(),
//         }
//     }

//     pub fn get_change_counter(&self) -> u32 {
//         self.change_counter.load(Ordering::Acquire)
//     }

//     /// Returns database size in pages.
//     pub fn get_database_size(&self) -> u32 {
//         self.database_size.load(Ordering::Acquire)
//     }

//     /// Returns "head" of linked list of free pages.
//     pub fn get_first_freelist_page(&self) -> PageNumber {
//         self.first_freelist_page.load(Ordering::Acquire)
//     }

//     /// Returns number of free pages, that can be reused.
//     pub fn get_freelist_pages(&self) -> PageNumber {
//         self.freelist_pages.load(Ordering::Acquire)
//     }

//     pub fn increment_change_counter(&self) {
//         self.change_counter.fetch_add(1, Ordering::Release);
//     }

//     /// Adds to current db size. Returns previous value.
//     pub fn add_database_size(&self, add: u32) -> u32 {
//         self.database_size.fetch_add(add, Ordering::Release)
//     }

//     pub fn set_first_freelist_page(&self, page_number: PageNumber) {
//         self.first_freelist_page
//             .store(page_number, Ordering::Release);
//     }

//     pub fn sub_freelist_pages(&self, sub: u32) {
//         let _ = self
//             .freelist_pages
//             .fetch_update(Ordering::Acquire, Ordering::Acquire, |val| {
//                 Some(val.saturating_sub(sub))
//             });
//     }

//     pub fn add_freelist_pages(&self, add: u32) {
//         self.freelist_pages.fetch_add(add, Ordering::Release);
//     }

//     pub fn get_version_valid_for(&self) -> u32 {
//         self.version_valid_for.load(Ordering::Acquire)
//     }

//     pub fn set_version_valid_for(&self, value: u32) {
//         self.version_valid_for.store(value, Ordering::Release);
//     }

//     /// Returns true if database is in complete state and doesn't need to
//     /// replay WAL. Otherwise returns false.
//     pub fn is_consistent(&self) -> bool {
//         self.get_change_counter() == self.get_version_valid_for()
//     }
// }

// impl From<DatabaseHeader> for MemDatabaseHeader {
//     fn from(db_header: DatabaseHeader) -> Self {
//         Self {
//             version: db_header.version,
//             page_size: db_header.page_size,
//             reserved_space: db_header.reserved_space,
//             change_counter: AtomicU32::new(db_header.change_counter),
//             database_size: AtomicU32::new(db_header.database_size),
//             first_freelist_page: AtomicU32::new(db_header.first_freelist_page),
//             freelist_pages: AtomicU32::new(db_header.freelist_pages),
//             default_page_cache_size: db_header.default_page_cache_size,
//             version_valid_for: AtomicU32::new(db_header.version_valid_for),
//         }
//     }
// }

pub struct Database {
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
    pub fn open(path: impl AsRef<Path>) -> DatabaseResult<Self> {
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

        Ok(Self { pager })
    }

    pub fn begin_read<'a>(&'a self) -> DatabaseResult<DatabaseReadTransaction> {
        let wal_tx = self.pager.wal.begin_read_tx()?;

        Ok(DatabaseReadTransaction {
            wal_tx: Rc::new(RefCell::new(wal_tx)),
            pager: self.pager.clone(),
        })
    }

    pub fn begin_write<'a>(&'a self) -> DatabaseResult<DatabaseWriteTransaction<'a>> {
        let wal_tx = self.pager.wal.begin_write_tx()?;

        Ok(DatabaseWriteTransaction {
            wal_tx: Rc::new(RefCell::new(wal_tx)),
            pager: self.pager.clone(),
        })
    }
}

pub struct DatabaseReadTransaction {
    wal_tx: Rc<RefCell<ReadTransaction>>,
    pager: Arc<Pager>,
}

impl DatabaseReadTransaction {
    pub fn cursor(&self, root: PageNumber) -> BTreeCursor<'_, ReadTransaction> {
        BTreeCursor::new(self.wal_tx.clone(), &self.pager, root)
    }

    #[must_use]
    pub fn commit(self) -> DatabaseResult<()> {
        Ok(())
    }
}

pub struct DatabaseWriteTransaction<'tx> {
    wal_tx: Rc<RefCell<WriteTransaction<'tx>>>,
    pager: Arc<Pager>,
}

impl<'tx> DatabaseWriteTransaction<'tx> {
    pub fn cursor(&self, root: PageNumber) -> BTreeCursor<'_, WriteTransaction<'tx>> {
        BTreeCursor::new(self.wal_tx.clone(), &self.pager, root)
    }

    /// Flushes dirty pages into WAL and marks them as clean. Might run checkpoint.
    #[must_use]
    pub fn commit(self) -> DatabaseResult<()> {
        let mut wal_tx = Rc::into_inner(self.wal_tx)
            .expect("Something is still using this transaction")
            .into_inner();

        self.pager.flush_dirty(&mut wal_tx, true)?;

        self.pager.wal.commit(&mut wal_tx)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        sql::{
            record::RecordBuilder,
            types::{AsValueRef, Value, text::Text},
        },
        storage::btree::{BTreeKey, DatabaseCursor},
    };

    const KEYS_START: i64 = 1;
    const KEYS_END: i64 = 20_000;

    #[test]
    fn test_insert() -> anyhow::Result<()> {
        simple_logger::init_with_level(log::Level::Trace)?;

        let db = Database::open("./test-db.db")?;

        let tx = db.begin_write()?;

        {
            // scope cursor to drop before tx commit.
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            let start = KEYS_START;
            let end = KEYS_END;

            for i in start..end {
                let mut record = RecordBuilder::new();

                record.add(Value::Null);
                record.add(Value::Int(i));
                record.add(Value::Text(Text::new(format!(
                    "Maciek Kowalski {i} i Antoni Kowalski {i}"
                ))));

                let record = record.serialize_to_record();

                log::trace!("inserting key: {}", i);

                cursor.insert(BTreeKey::new_table_key(i, Some(record)))?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_search() -> anyhow::Result<()> {
        simple_logger::init()?;

        let db = Database::open("./test-db.db")?;

        let tx = db.begin_read()?;

        {
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            for i in KEYS_START..KEYS_END {
                assert!(
                    cursor.seek(&BTreeKey::new_table_key(i, None))?.is_found(),
                    "Entry {} lost",
                    i
                );
                if i % 1_000 == 0 {
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
        simple_logger::init_with_level(log::Level::Info)?;

        let db = Database::open("./test-db.db")?;

        let tx = db.begin_read()?;

        {
            let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

            let mut count = 0;

            while cursor.next()? {
                // if let Ok(record) = cursor.try_record() {
                //     if record.get_value(1).to_int() == 84 {
                //         log::info!("Found it!");
                //     }
                // }
                count += 1;
            }

            log::info!("Scaned entries: {}", count);
        }

        tx.commit()?;

        Ok(())
    }
}
