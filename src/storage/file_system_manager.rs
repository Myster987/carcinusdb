use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use super::heap::{BlockId, BlockNumber};

/// Manages paths
pub struct FileSystemManager {
    base_dir: PathBuf,
}

impl FileSystemManager {
    pub fn new(base_dir: &str) -> Self {
        Self {
            base_dir: PathBuf::from_str(base_dir).expect("Could parse carcinusdb base dir."),
        }
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.base_dir.join("metadata")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.base_dir.join("data")
    }

    pub fn database_dir(&self, db_name: &str) -> PathBuf {
        self.data_dir().join(db_name)
    }

    pub fn schema_path(&self, db_name: &str) -> PathBuf {
        self.database_dir(db_name).join("schema")
    }

    pub fn table_block_path(
        &self,
        db_path: &Path,
        table_id: BlockId,
        block_number: BlockNumber,
    ) -> PathBuf {
        db_path.join(format!("{table_id}.{block_number}"))
    }

    pub fn index_block_path(
        &self,
        db_path: &Path,
        table_id: BlockId,
        block_number: BlockNumber,
    ) -> PathBuf {
        db_path.join(format!("{table_id}_idx.{block_number}"))
    }

    pub fn table_fsm_path(&self, db_path: &Path, table_id: BlockId) -> PathBuf {
        db_path.join(format!("{table_id}_fsm"))
    }

    pub fn index_fsm_path(&self, db_path: &Path, table_id: BlockId) -> PathBuf {
        db_path.join(format!("{table_id}_idx_fsm"))
    }
}
