use std::{env, path::PathBuf};

use crate::{error::DatabaseResult, storage::file_system_manager::FileSystemManager};

#[derive(Debug)]
pub struct Database {
    file_system_manager: FileSystemManager,
}

impl Database {
    pub fn init(database_name: &str) -> DatabaseResult<Self> {
        let env_base_path = env::var("CARNICUSDB_BASE_PATH")?;
        let base_path = PathBuf::from(env_base_path);

        let file_system_manager = FileSystemManager::new(base_path, database_name);

        Ok(Self {
            file_system_manager,
        })
    }
}
