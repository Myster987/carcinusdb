use std::{
    cmp::Ordering,
    collections::VecDeque,
    io::{self, BufReader, Read, Seek, Write},
};

use crate::vm::{
    self,
    operator::{Operator, Row},
};

const ROW_PAGE_HEADER_SIZE: usize = size_of::<u32>();

pub struct RowBuffer {
    /// Max size of `page` (fixed size block of records) in bytes.
    page_size: usize,

    /// Current size of this buffer in bytes.
    current_size: usize,

    /// Size in bytes of largest records that has ever been stored in
    /// this buffer.
    largest_record_size: usize,

    /// Buffer mode.
    packed: bool,

    rows: VecDeque<Row>,
}

impl RowBuffer {
    /// Empty buffer used to only replace other ones with `mem::replace`.
    pub fn empty() -> Self {
        Self {
            page_size: 0,
            current_size: 0,
            largest_record_size: 0,
            packed: false,
            rows: VecDeque::new(),
        }
    }

    /// Creates new buffer. No allocation yet.
    pub fn new(page_size: usize, packed: bool) -> Self {
        Self {
            page_size,
            packed,
            current_size: if packed { 0 } else { ROW_PAGE_HEADER_SIZE },
            largest_record_size: 0,
            rows: VecDeque::new(),
        }
    }

    pub fn can_fit(&self, row: &Row) -> bool {
        self.current_size + row.size() <= self.page_size
    }

    pub fn push(&mut self, row: Row) {
        let row_size = row.size();

        if row_size > self.largest_record_size {
            self.largest_record_size = row_size;
        }

        self.current_size += row_size;
        self.rows.push_back(row);
    }

    pub fn pop_front(&mut self) -> Option<Row> {
        self.rows.pop_front().inspect(|row| {
            self.current_size -= row.size();
        })
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.current_size = if self.packed { 0 } else { ROW_PAGE_HEADER_SIZE };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.page_size);

        // Page header.
        if !self.packed {
            buf.extend_from_slice(&(self.rows.len() as u32).to_le_bytes());
        }

        // Tuples.
        for row in &self.rows {
            buf.extend_from_slice(row.raw());
        }

        // Padding.
        if !self.packed {
            buf.resize(self.page_size, 0);
        }

        buf
    }

    pub fn write_to(&self, file: &mut impl Write) -> io::Result<()> {
        file.write_all(&self.serialize())
    }

    pub fn read_from(&mut self, file: &mut impl Read) -> vm::Result<()> {
        debug_assert!(self.is_empty() && !self.packed);

        let mut buffer = vec![0; self.page_size];
        file.read_exact(&mut buffer)
            .map_err(|e| crate::storage::Error::Io(e))?;

        let number_of_rows = u32::from_le_bytes(
            buffer[..ROW_PAGE_HEADER_SIZE]
                .try_into()
                .map_err(|_| vm::Error::Corrupted)?,
        );

        let mut cursor = ROW_PAGE_HEADER_SIZE;

        for _ in 0..number_of_rows {
            let row = Row::deserialize(&buffer[cursor..])?;
            cursor += row.size();
            self.push(row);
        }

        Ok(())
    }

    pub fn read_page(&mut self, file: &mut (impl Seek + Read), page: usize) -> vm::Result<()> {
        file.seek(io::SeekFrom::Start((self.page_size * page) as u64))
            .map_err(|_| vm::Error::Corrupted)?;
        self.read_from(file)
    }

    pub fn page_size_needed_for(row_size: usize) -> usize {
        let mut page_size = size_of::<u32>() * 2 + row_size;
        page_size -= 1;
        page_size |= page_size >> 1;
        page_size |= page_size >> 2;
        page_size |= page_size >> 4;
        page_size |= page_size >> 8;
        page_size |= page_size >> 16;
        page_size += 1;

        page_size
    }

    pub fn sort_by(&mut self, cmp: impl FnMut(&Row, &Row) -> Ordering) {
        self.rows.make_contiguous().sort_by(cmp);
    }
}
