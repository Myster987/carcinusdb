use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::LazyLock,
};

use crate::storage::Oid;

use super::heap::BlockNumber;

pub const FILE_SYSTEM_MANAGER: LazyLock<FileSystemManager> = LazyLock::new(|| {
    let base_dir =
        std::env::var("CARCINUSDB_BASE").expect("CARCINUS_BASE env variable not configured");
    FileSystemManager::new(&base_dir)
});

/// Manages paths
#[derive(Debug)]
pub struct FileSystemManager {
    base_dir: PathBuf,
}

impl FileSystemManager {
    pub fn new(base_dir: &str) -> Self {
        Self {
            base_dir: PathBuf::from_str(base_dir).expect("Could parse carcinusdb base dir."),
        }
    }

    pub fn base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.base_dir.join("metadata")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.base_dir.join("data")
    }

    pub fn database_dir(&self, db_oid: Oid) -> PathBuf {
        self.data_dir().join(format!("{db_oid}"))
    }

    pub fn schema_path(&self, db_oid: Oid) -> PathBuf {
        self.database_dir(db_oid).join("schema")
    }

    pub fn table_catalog_path(&self, db_oid: Oid) -> PathBuf {
        self.database_dir(db_oid).join("table")
    }

    pub fn index_catalog_path(&self, db_oid: Oid) -> PathBuf {
        self.database_dir(db_oid).join("index")
    }

    pub fn table_block_path(
        &self,
        db_path: &Path,
        table_id: Oid,
        block_number: BlockNumber,
    ) -> PathBuf {
        db_path.join(format!("{table_id}.{block_number}"))
    }

    pub fn index_block_path(
        &self,
        db_path: &Path,
        index_id: Oid,
        block_number: BlockNumber,
    ) -> PathBuf {
        db_path.join(format!("{index_id}_idx.{block_number}"))
    }

    pub fn table_fsm_path(&self, db_path: &Path, table_id: Oid) -> PathBuf {
        db_path.join(format!("{table_id}_fsm"))
    }

    pub fn index_fsm_path(&self, db_path: &Path, table_id: Oid) -> PathBuf {
        db_path.join(format!("{table_id}_idx_fsm"))
    }
}
