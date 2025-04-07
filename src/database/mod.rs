use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::{Path, PathBuf},
};

use bytes::{Buf, BytesMut};
use config::Config;

use crate::{error::DatabaseResult, pager::Pager, storage::page::{ConfigPage, CONFIG_PAGE_SIZE}};

pub mod config;

#[derive(Debug)]
pub struct Database {
    config: Config,
    pager: Pager
}

impl Database {
    pub fn init(path: impl AsRef<Path>) -> DatabaseResult<Self> {
        let pager = Pager::new(path)?;        
        // let mut reader = BufReader::new(file);
        
    
        let _ = reader.read(&mut buffer)?;
        let mut cursor = Cursor::new(&buffer[..]);

        let config_page = ConfigPage::from_bytes(&mut cursor)?;
        
        Ok(Self {
            config: Config::from(config_page)
        })
    }

    // pub fn read_page_zero(&self) -> DatabaseResult<PageZero> {
    //     let mut file = File::open(self.work_dir)?;

    // }
}
