use std::{collections::HashMap, fs::File};

use crate::{
    storage::{Oid, PageNumber},
    utils::cast,
};

const TABLE_SIZE: usize = size_of::<Table>();

/// Map of tables in database
#[derive(Debug)]
pub struct TableCatalog {
    file: File,
    tables: HashMap<Oid, Table>,
}

impl TableCatalog {
    // pub fn from_file(mut file: File, num_of_tables: usize) -> Result<Self> {
    //     let mut buffer = vec![0; TABLE_ON_DISK_SIZE];
    //     let mut tables = HashMap::new();

    //     for i in 0..num_of_tables {
    //         file.seek(SeekFrom::Start((i * TABLE_ON_DISK_SIZE) as u64))?;
    //         file.read(&mut buffer)?;
    //         let table = Table::from_buffer(&buffer)?;
    //         tables.insert(table.oid, table);
    //     }

    //     Ok(TableCatalog { file, tables })
    // }

    // pub fn sync(&self) -> Result<()> {
    //     for table in self.tables.values() {}

    //     Ok(())
    // }
}

/// Table catalog. On disk size: 272 bytes
///
/// **Order on disk (name, offset, size):**
///
/// - oid, 0, 4
/// - namespace, 4, 4
/// - estimated_pages, 8, 4
/// - estimated_rows, 12, 8
/// - name, 20, 63 * 4 = 252
///
/// **Total: 272**
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Table {
    oid: Oid,
    /// utf-8 encoded string. assuming worst case that, each character will take 4 bytesm, max len 63 chars. trailing spaces and new line symbols will be trimmed
    name: [u8; 252],
    namespace: Oid,
    estimated_rows: u64,
    estimated_pages: PageNumber,
}

impl Table {
    pub fn name(&self) -> &str {
        // unsafe { std::slice::from_raw_parts(self.name, self.name.len()) }
        std::str::from_utf8(&self.name).unwrap()
    }
    pub fn as_bytes(&self) -> &[u8] {
        // unsafe { std::slice::from_raw_parts((self as *const Table) as *const u8, TABLE_SIZE) }
        cast::bytes_of(self)
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        cast::bytes_of_mut(self)
    }

    pub fn from_bytes(data: &[u8]) -> &Self {
        // let table = ptr::slice_from_raw_parts(data.as_ptr(), TABLE_SIZE) as *mut Table;
        // unsafe { NonNull::new_unchecked(table).as_ref() }
        cast::from_bytes(data)
    }

    pub fn from_bytes_mut(data: &mut [u8]) -> &mut Self {
        cast::from_bytes_mut(data)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn main_test() -> anyhow::Result<()> {
        let name = {
            let s = b"Maciek";
            let mut arr = [0; 252];
            arr[..s.len()].copy_from_slice(s);
            arr
        };
        let table = Table {
            oid: 1,
            name,
            estimated_pages: 10,
            estimated_rows: 700,
            namespace: 3,
        };


        let table_2 = Table::from_bytes(table.as_bytes());

        assert_eq!(table, *table_2);

        Ok(())
    }
}
