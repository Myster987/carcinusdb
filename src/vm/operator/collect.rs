use std::{
    cmp::Ordering,
    collections::VecDeque,
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, Write},
};

use crate::{
    sql::record::Record,
    vm::{self, operator::Operator},
};

const RECORD_PAGE_HEADER_SIZE: usize = size_of::<u32>();
const RECORD_HEADER_SIZE: usize = size_of::<u32>();

pub struct RecordBuffer {
    /// Max size of `page` (fixed size block of records) in bytes.
    page_size: usize,

    /// Current size of this buffer in bytes.
    current_size: usize,

    /// Size in bytes of largest records that has ever been stored in
    /// this buffer.
    largest_record_size: usize,

    /// Buffer mode.
    packed: bool,

    records: VecDeque<Record>,
}

impl RecordBuffer {
    /// Empty buffer used to only replace other ones with `mem::replace`.
    pub fn empty() -> Self {
        Self {
            page_size: 0,
            current_size: 0,
            largest_record_size: 0,
            packed: false,
            records: VecDeque::new(),
        }
    }

    /// Creates new buffer. No allocation yet.
    pub fn new(page_size: usize, packed: bool) -> Self {
        Self {
            page_size,
            packed,
            current_size: if packed { 0 } else { RECORD_PAGE_HEADER_SIZE },
            largest_record_size: 0,
            records: VecDeque::new(),
        }
    }

    pub fn can_fit(&self, record: &Record) -> bool {
        self.current_size + RECORD_HEADER_SIZE + record.size() <= self.page_size
    }

    pub fn push(&mut self, record: Record) {
        let record_size = RECORD_HEADER_SIZE + record.size();

        if record_size > self.largest_record_size {
            self.largest_record_size = record_size;
        }

        self.current_size += record_size;
        self.records.push_back(record);
    }

    pub fn pop_front(&mut self) -> Option<Record> {
        self.records.pop_front().inspect(|record| {
            self.current_size -= record.size();
        })
    }

    pub fn peek_front(&mut self) -> Option<Record> {
        self.records
            .iter()
            .peekable()
            .peek()
            .map(|&r| r.to_borrowed())
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn clear(&mut self) {
        self.records.clear();
        self.current_size = if self.packed {
            0
        } else {
            RECORD_PAGE_HEADER_SIZE
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.page_size);

        // Page header.
        if !self.packed {
            buf.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        }

        // Records.
        for record in &self.records {
            buf.extend_from_slice(&(record.size() as u32).to_le_bytes());
            buf.extend_from_slice(record.raw());
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
            buffer[..RECORD_PAGE_HEADER_SIZE]
                .try_into()
                .map_err(|_| vm::Error::Corrupted)?,
        );

        let mut cursor = RECORD_PAGE_HEADER_SIZE;

        for _ in 0..number_of_rows {
            let record_size = u32::from_le_bytes(
                buffer[cursor..cursor + RECORD_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            );
            cursor += RECORD_HEADER_SIZE;
            let record = Record::deserialize(&buffer[cursor..cursor + record_size as usize])?;
            cursor += record.size();
            self.push(record);
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

    pub fn sort_by(&mut self, cmp: impl FnMut(&Record, &Record) -> Ordering) {
        self.records.make_contiguous().sort_by(cmp);
    }
}

pub struct Collect<'tx> {
    source: Box<dyn Operator + 'tx>,

    /// `true` if iterator is empty.
    collected: bool,

    /// In memory record buffer.
    mem_buffer: RecordBuffer,
    /// If records can't fit in memory they are stored in temp file.
    file: Option<File>,

    /// If records can't fit in memory then reader is needed.
    reader: Option<BufReader<File>>,
}

impl<'tx> Collect<'tx> {
    pub fn new(source: Box<dyn Operator + 'tx>, mem_buffer_size: usize, packed: bool) -> Self {
        Self {
            source,
            collected: false,
            mem_buffer: RecordBuffer::new(mem_buffer_size, packed),
            file: None,
            reader: None,
        }
    }

    fn collect(&mut self) -> vm::Result<()> {
        while let Some(record) = self.source.next()? {
            if !self.mem_buffer.can_fit(&record) {
                if self.file.is_none() {
                    let temp_file = tempfile::tempfile().map_err(|_| vm::Error::Corrupted)?;
                    self.file = Some(temp_file);
                }
                self.mem_buffer
                    .write_to(self.file.as_mut().unwrap())
                    .map_err(|_| vm::Error::Corrupted)?;
                self.mem_buffer.clear();

                if !self.mem_buffer.can_fit(&record) {
                    panic!(
                        "Record was bigger than Page, so it can't be sorted now. This needs TOAST-like system to handle it. Which is not implemented"
                    );
                }
            }

            self.mem_buffer.push(record);
        }

        if let Some(mut file) = self.file.take() {
            file.rewind().map_err(|_| vm::Error::Corrupted)?;
            self.reader = Some(BufReader::with_capacity(self.mem_buffer.page_size, file));
        }

        Ok(())
    }

    pub fn peek(&mut self) -> vm::Result<Option<Record>> {
        if !self.collected {
            self.collect()?;
            self.collected = true;
        }

        if let Some(reader) = self.reader.as_mut() {
            let has_data_left = reader
                .fill_buf()
                .map(|r| !r.is_empty())
                .map_err(|_| vm::Error::Corrupted)?;

            if has_data_left {
                let record_size = &mut [0; 4];
                reader
                    .read_exact(record_size)
                    .map_err(|_| vm::Error::Corrupted)?;
                let record_size = u32::from_le_bytes(record_size[..].try_into().unwrap()) as usize;

                let mut record_buf = vec![0; record_size];
                reader
                    .read_exact(&mut record_buf)
                    .map_err(|_| vm::Error::Corrupted)?;

                reader
                    .seek_relative((record_size + 4) as i64 * -1)
                    .map_err(|_| vm::Error::Corrupted)?;

                return Ok(Some(Record::new(record_buf)));
            }
        }

        Ok(self.mem_buffer.peek_front())
    }
}

impl<'tx> Operator for Collect<'tx> {
    fn schema(&self) -> &crate::sql::schema::Schema {
        self.source.schema()
    }

    fn next(&mut self) -> vm::Result<Option<Record>> {
        if !self.collected {
            self.collect()?;
            self.collected = true;
        }

        if let Some(reader) = self.reader.as_mut() {
            let has_data_left = reader
                .fill_buf()
                .map(|r| !r.is_empty())
                .map_err(|_| vm::Error::Corrupted)?;

            if has_data_left {
                let record_size = &mut [0; 4];
                reader
                    .read_exact(record_size)
                    .map_err(|_| vm::Error::Corrupted)?;
                let record_size = u32::from_le_bytes(record_size[..].try_into().unwrap()) as usize;

                let mut record_buf = vec![0; record_size];
                reader
                    .read_exact(&mut record_buf)
                    .map_err(|_| vm::Error::Corrupted)?;

                return Ok(Some(Record::new(record_buf)));
            }
        }

        Ok(self.mem_buffer.pop_front())
    }
}
