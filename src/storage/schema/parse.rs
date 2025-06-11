use std::{fs::File, io::{BufReader, Cursor, Read}, path::PathBuf};

use bytes::BytesMut;

use crate::{storage::{file_system_manager::FILE_SYSTEM_MANAGER, schema::{ResourceType, Schema}, Result}, utils::bytes::get_u8};


pub fn parse_schema(db_name: &str) -> Result<Schema> {
    todo!()
    // let schema_path = FILE_SYSTEM_MANAGER.schema_path(db_name);
    // let file = File::open(schema_path)?;

    // let reader = BufReader::new(file);
    // let mut buffer = Vec::new();

    // reader.read_to_end(&mut buffer)?;

    // let cursor = Cursor::new(buffer);

    // let db_name_len = get_u8(&mut cursor).unwrap();
    
    
}