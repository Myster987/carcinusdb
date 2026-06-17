use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

use crate::{
    sql::{parser::statement::Expression, record::Record},
    vm::{
        self,
        operator::{Operator, collect::Collect},
    },
};

pub struct Sort<'tx> {
    sorted: bool,
    page_size: usize,
    buffer_mem: usize,
    k_way: usize,

    order_by: Vec<Expression>,
    collect: Collect<'tx>,

    sorted_run: Option<SortRunReader>,
}

impl<'tx> Sort<'tx> {
    pub fn new(
        order_by: Vec<Expression>,
        collect: Collect<'tx>,
        page_size: usize,
        buffer_mem: usize,
        k_way: usize,
    ) -> Self {
        Self {
            sorted: false,
            page_size,
            buffer_mem,
            k_way,
            order_by,
            collect,
            sorted_run: None,
        }
    }

    fn generate_runs(&mut self) -> vm::Result<Vec<SortRunReader>> {
        let mut sort_runs = Vec::new();
        let mut sort_heap = RecordHeap::new(self.buffer_mem);

        // RUN 0

        sort_heap.load_to_max_size(&mut self.collect)?;

        while let Some(new_record) = self.collect.next()? {
            let mut run = SortRun::new();

            while !sort_heap.is_frozen() {
                loop {
                    // loop if next is too big
                    let record_to_write = sort_heap.pop().unwrap();

                    run.write_record(record_to_write)?;

                    if sort_heap.push(new_record.clone()).is_some() {
                        break;
                    }
                }
            }

            // should produce single sorted run.
            sort_runs.push(SortRunReader::from_run(run)?);
            sort_heap.unfreez();
            sort_heap.load_to_max_size(&mut self.collect)?;
        }

        if !sort_heap.is_empty() {
            let mut run = SortRun::new();

            while let Some(record_to_write) = sort_heap.pop() {
                run.write_record(record_to_write)?;

                if !sort_heap.is_empty() {
                    sort_heap.unfreez();
                }
            }

            sort_runs.push(SortRunReader::from_run(run)?);
        }

        Ok(sort_runs)
    }

    fn merge(&mut self, readers: Vec<SortRunReader>) -> vm::Result<Vec<SortRunReader>> {
        // if only 1 run is left we have sorted and combined whole data.
        if readers.len() <= 1 {
            return Ok(readers);
        }

        let mut merged_runs = Vec::new();

        for mut chunk in crate::utils::bytes::into_chunks(readers, self.k_way) {
            let mut heap = BinaryHeap::new();

            for (i, reader) in chunk.iter_mut().enumerate() {
                if let Some(record) = reader.next_record()? {
                    heap.push(Reverse(MergeEntry { record, run_idx: i }));
                }
            }

            let mut merge_state = MergeState {
                readers: chunk,
                heap,
            };

            let mut run = SortRun::new();

            while let Some(Reverse(MergeEntry { record, run_idx })) = merge_state.heap.pop() {
                run.write_record(record)?;
                if let Some(next) = merge_state.readers[run_idx].next_record()? {
                    merge_state.heap.push(Reverse(MergeEntry {
                        record: next,
                        run_idx,
                    }));
                }
            }

            merged_runs.push(SortRunReader::from_run(run)?);
        }

        self.merge(merged_runs)
    }
}

impl<'tx> Operator for Sort<'tx> {
    fn schema(&self) -> &crate::sql::schema::Schema {
        self.collect.schema()
    }

    fn next(&mut self) -> vm::Result<Option<Record>> {
        if !self.sorted {
            let readers = self.generate_runs()?;

            // by how merge works we know that it will contain 1 or 0 elements.
            self.sorted_run = self.merge(readers)?.pop();
            self.sorted = true;
        }

        match self.sorted_run.as_mut() {
            Some(run) => run.next_record(),
            None => Ok(None),
        }
    }
}

struct SortRun {
    writer: BufWriter<File>,
}

impl SortRun {
    fn new() -> Self {
        Self {
            writer: BufWriter::new(tempfile::tempfile().unwrap()),
        }
    }

    pub fn write_record(&mut self, record: Record) -> vm::Result<()> {
        let record_size = (record.size() as u32).to_le_bytes();
        self.writer
            .write_all(&record_size)
            .map_err(|_| vm::Error::Corrupted)?;
        self.writer
            .write_all(record.raw())
            .map_err(|_| vm::Error::Corrupted)?;
        Ok(())
    }
}

struct RecordHeap {
    heap: BinaryHeap<Reverse<Record>>,

    /// Records with `frozen` status.
    freeze_list: Vec<Record>,

    last_pop: Option<Record>,

    /// Current size of total heap + list size in `bytes`.
    current_size: usize,

    /// Total alloved size of heap + list size in `bytes`.
    max_size: usize,
}

impl RecordHeap {
    fn new(max_size: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            freeze_list: Vec::new(),

            last_pop: None,

            current_size: 0,
            max_size,
        }
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty() && self.freeze_list.is_empty()
    }

    fn is_frozen(&self) -> bool {
        self.heap.is_empty() && !self.freeze_list.is_empty()
    }

    fn load_to_max_size(&mut self, record_src: &mut Collect<'_>) -> vm::Result<()> {
        while let Some(record) = record_src.peek()?
            && self.current_size + record.size() <= self.max_size
        {
            self.current_size += record.size();
            self.heap.push(Reverse(record));
            let _ = record_src.next();
        }

        Ok(())
    }

    fn unfreez(&mut self) {
        let old_vec = std::mem::replace(&mut self.freeze_list, Vec::new());
        self.heap = BinaryHeap::from_iter(old_vec.into_iter().map(|r| Reverse(r)));
        self.last_pop = None;
    }

    fn can_fit(&self, record: &Record) -> bool {
        self.current_size + record.size() <= self.max_size
    }

    fn pop(&mut self) -> Option<Record> {
        let popped = self.heap.pop().map(|Reverse(r)| {
            self.current_size -= r.size();
            r
        });
        self.last_pop = popped.clone();
        popped
    }

    fn push(&mut self, new_record: Record) -> Option<()> {
        if self.current_size + new_record.size() > self.max_size {
            return None;
        }

        self.current_size += new_record.size();
        if let Some(last_popped) = &self.last_pop {
            if &new_record < last_popped {
                self.freeze_list.push(new_record);
            } else {
                self.heap.push(Reverse(new_record));
            }
        } else {
            self.heap.push(Reverse(new_record));
        }

        Some(())
    }
}

struct SortRunReader {
    reader: BufReader<File>,
}

impl SortRunReader {
    pub fn from_run(run: SortRun) -> vm::Result<Self> {
        let mut file = run.writer.into_inner().map_err(|_| vm::Error::Corrupted)?;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| vm::Error::Corrupted)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn next_record(&mut self) -> vm::Result<Option<Record>> {
        let mut record_size = [0u8; 4];
        match self.reader.read_exact(&mut record_size) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(_) => return Err(vm::Error::Corrupted),
        };

        let record_size = u32::from_le_bytes(record_size) as usize;
        let mut buf = vec![0; record_size];

        self.reader
            .read_exact(&mut buf)
            .map_err(|_| vm::Error::Corrupted)?;
        Ok(Some(Record::new(buf)))
    }
}

struct MergeEntry {
    record: Record,
    run_idx: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, o: &Self) -> bool {
        self.record == o.record
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, o: &Self) -> Ordering {
        self.record.cmp(&o.record)
    }
}

struct MergeState {
    readers: Vec<SortRunReader>,
    heap: BinaryHeap<Reverse<MergeEntry>>,
}
