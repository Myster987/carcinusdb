use std::fs;

use crate::{error::DatabaseResult, storage::file_system_manager::FILE_SYSTEM_MANAGER};



pub fn create_base_dir() -> DatabaseResult<()> {
    let base = FILE_SYSTEM_MANAGER.base_dir();

    if !base.exists() {
        log::info!("CarcinusDB directory not detected. Creating one...");
        fs::create_dir(base)?;
    } else {
        log::info!("CarcinusDB directory detected at. {:?}", base);
    }

    Ok(())
}