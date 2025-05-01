use std::{
    io::{Cursor, Read},
    path::PathBuf,
};

use crate::{
    error::{DatabaseError, DatabaseResult},
    utils::bytes::get_u16,
};

/// Holds general information about db
pub struct Metadata {
    version: u16,
    db_name: String,
    page_size: u16,
    base: PathBuf,
}

impl TryFrom<&mut Cursor<&[u8]>> for Metadata {
    type Error = DatabaseError;

    fn try_from(value: &mut Cursor<&[u8]>) -> DatabaseResult<Self> {
        let version = get_u16(value)?;
        let page_size = get_u16(value)?;

        let db_name_length = get_u16(value)?;
        let mut buffer = vec![0; db_name_length as usize];

        value.read(&mut buffer[..])?;

        let db_name = String::from_utf8(buffer).expect("Invalid database name.");

        let base_path_length = get_u16(value)?;
        let mut buffer = vec![0; base_path_length as usize];

        value.read(&mut buffer[..]);

        let base_path =
            PathBuf::from(String::from_utf8(buffer).expect("Invalid database folder path."));

        Ok(Self {
            version,
            db_name,
            page_size,
            base: base_path,
        })
    }
}
