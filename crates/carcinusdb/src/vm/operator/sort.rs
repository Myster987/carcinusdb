use std::{
    cmp::Ordering,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

use binary_heap_plus::BinaryHeap;
use compare::Compare;

use crate::{
    sql::{
        parser::statement::{Expression, Order},
        record::{self, Record, RecordMut},
        schema::Schema,
    },
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

    order_by_expr: Vec<Expression>,
    total_order: Vec<Order>,

    collect: Collect<'tx>,

    sorted_run: Option<SortRunReader>,
}

impl<'tx> Sort<'tx> {
    pub fn new(
        collect: Collect<'tx>,
        order_by: Vec<(Expression, Order)>,
        page_size: usize,
        buffer_mem: usize,
        k_way: usize,
    ) -> Self {
        let mut order_by_expr = Vec::with_capacity(order_by.len());
        let mut total_order = Vec::with_capacity(order_by.len());

        for (expr, order) in order_by.into_iter() {
            order_by_expr.push(expr);
            total_order.push(order);
        }

        Self {
            sorted: false,
            page_size,
            buffer_mem,
            k_way,
            order_by_expr,
            total_order,
            collect,
            sorted_run: None,
        }
    }

    fn generate_runs(&mut self) -> vm::Result<Vec<SortRunReader>> {
        let schema = self.schema().clone();
        let mut sort_runs = Vec::new();
        let mut sort_heap =
            ReplacementSelectionHeap::new(self.buffer_mem, self.total_order.clone());

        // RUN 0

        sort_heap.load_to_max_size(&schema, &self.order_by_expr, &mut self.collect)?;

        let mut current_run = SortRun::new();

        while let Some(new_record) = self.collect.next()? {
            let new_entry = MergeEntry::new(self.schema(), &self.order_by_expr, new_record, 0)?;

            loop {
                match sort_heap.pop() {
                    Some(entry) => {
                        current_run.write_entry(entry)?;
                        if sort_heap.push(new_entry.clone()).is_some() {
                            break;
                        }
                    }
                    None => {
                        sort_heap.force_push(new_entry.clone());
                        break;
                    }
                }
            }

            if sort_heap.is_frozen() {
                // should produce single sorted run.
                sort_runs.push(SortRunReader::from_run(current_run)?);
                current_run = SortRun::new();
                sort_heap.unfreez();
                sort_heap.load_to_max_size(&schema, &self.order_by_expr, &mut self.collect)?;
            }
        }

        while let Some(record) = sort_heap.pop() {
            current_run.write_entry(record)?;
        }
        sort_runs.push(SortRunReader::from_run(current_run)?);

        if sort_heap.is_frozen() {
            sort_heap.unfreez();
            let mut frozen_run = SortRun::new();
            while let Some(record) = sort_heap.pop() {
                frozen_run.write_entry(record)?;
            }
            sort_runs.push(SortRunReader::from_run(frozen_run)?);
        }

        Ok(sort_runs)
    }

    fn merge(&mut self, readers: Vec<SortRunReader>) -> vm::Result<Vec<SortRunReader>> {
        // if only 1 run is left we have sorted and combined whole data.
        if readers.len() <= 1 {
            return Ok(readers);
        }

        let mut merged_runs = Vec::new();

        let cmp = MergeEntryCmp(self.total_order.clone());

        for mut chunk in crate::utils::bytes::into_chunks(readers, self.k_way) {
            let mut heap = BinaryHeap::from_vec_cmp(Vec::new(), cmp.clone());

            for (i, reader) in chunk.iter_mut().enumerate() {
                if let Some(mut entry) = reader.next_entry()? {
                    entry.run_idx = i;
                    heap.push(entry);
                }
            }

            let mut merge_state = MergeState {
                readers: chunk,
                heap,
            };

            let mut run = SortRun::new();

            while let Some(entry) = merge_state.heap.pop() {
                let run_idx = entry.run_idx;
                run.write_entry(entry)?;
                if let Some(mut next_entry) = merge_state.readers[run_idx].next_entry()? {
                    next_entry.run_idx = run_idx;
                    merge_state.heap.push(next_entry);
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
            Some(run) => run.next_entry().map(|entry| entry.map(|e| e.record)),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
struct MergeEntryCmp(Vec<Order>);

impl compare::Compare<MergeEntry> for MergeEntryCmp {
    fn compare(&self, l: &MergeEntry, r: &MergeEntry) -> Ordering {
        record::compare_records_custom_order(&l.key, &r.key, &self.0).reverse()
    }
}

/// Helps to reduce initial count of merge runs. On avarege it's something
/// arround `2M`, where `M` is size of single run.
struct ReplacementSelectionHeap {
    /// Compare method used by heap. Can either be in `ASC` or `DESC` order.
    cmp: MergeEntryCmp,
    heap: BinaryHeap<MergeEntry, MergeEntryCmp>,
    /// Records with `frozen` status.
    freeze_list: Vec<MergeEntry>,
    /// Latest removed entry from heap.
    last_pop: Option<MergeEntry>,
    /// Current size of heap in **bytes**.
    heap_size: usize,
    /// Current size of freeze list in **bytes**.
    frozen_size: usize,
    /// Total alloved size of heap + list size in `bytes`.
    max_size: usize,
}

impl ReplacementSelectionHeap {
    fn new(max_size: usize, orders: Vec<Order>) -> Self {
        let cmp = MergeEntryCmp(orders);

        let heap = BinaryHeap::from_vec_cmp(Vec::new(), cmp.clone());

        Self {
            cmp,
            heap,
            freeze_list: Vec::new(),
            last_pop: None,
            heap_size: 0,
            frozen_size: 0,
            max_size,
        }
    }

    fn is_frozen(&self) -> bool {
        self.heap_size == 0 && self.frozen_size > 0
    }

    fn pop(&mut self) -> Option<MergeEntry> {
        let popped = self.heap.pop().inspect(|entry| {
            self.heap_size -= entry.size();
            self.last_pop = Some(entry.clone());
        });
        popped
    }

    fn push(&mut self, new_entry: MergeEntry) -> Option<()> {
        let freezes = self
            .last_pop
            .as_ref()
            .map(|last| self.cmp.compare(&new_entry, last) == Ordering::Greater)
            .unwrap_or(false);

        if freezes {
            self.frozen_size += new_entry.size();
            self.freeze_list.push(new_entry);
        } else {
            if self.heap_size + new_entry.size() > self.max_size {
                return None;
            }
            self.heap_size += new_entry.size();
            self.heap.push(new_entry);
        }
        Some(())
    }

    fn force_push(&mut self, new_entry: MergeEntry) {
        let freezes = self
            .last_pop
            .as_ref()
            .map(|last| self.cmp.compare(&new_entry, last) == Ordering::Greater)
            .unwrap_or(false);
        if freezes {
            self.frozen_size += new_entry.size();
            self.freeze_list.push(new_entry);
        } else {
            self.heap_size += new_entry.size();
            self.heap.push(new_entry);
        }
    }

    fn load_to_max_size(
        &mut self,
        schema: &Schema,
        expressions: &[Expression],
        record_src: &mut Collect<'_>,
    ) -> vm::Result<()> {
        while let Some(record) = record_src.peek()? {
            let entry = MergeEntry::new(schema, expressions, record, 0)?;
            if self.heap_size + entry.size() > self.max_size {
                break;
            }
            self.heap_size += entry.size();
            self.heap.push(entry);
            let _ = record_src.next();
        }

        Ok(())
    }

    fn unfreez(&mut self) {
        let old_vec = std::mem::replace(&mut self.freeze_list, Vec::new());
        self.heap = BinaryHeap::from_vec_cmp(old_vec, self.cmp.clone());
        self.heap_size = self.frozen_size;
        self.frozen_size = 0;
        self.last_pop = None;
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

    pub fn write_entry(&mut self, entry: MergeEntry) -> vm::Result<()> {
        let key_size = (entry.key.size() as u32).to_le_bytes();
        let record_size = (entry.record.size() as u32).to_le_bytes();

        self.writer
            .write_all(&key_size)
            .map_err(|_| vm::Error::Corrupted)?;
        self.writer
            .write_all(&record_size)
            .map_err(|_| vm::Error::Corrupted)?;

        self.writer
            .write_all(entry.key.raw())
            .map_err(|_| vm::Error::Corrupted)?;
        self.writer
            .write_all(entry.record.raw())
            .map_err(|_| vm::Error::Corrupted)?;

        Ok(())
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

    fn next_entry(&mut self) -> vm::Result<Option<MergeEntry>> {
        let mut key_and_record_size = [0u8; 8];
        match self.reader.read_exact(&mut key_and_record_size) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(_) => return Err(vm::Error::Corrupted),
        };

        let key_size = u32::from_le_bytes(key_and_record_size[..4].try_into().unwrap()) as usize;
        let record_size = u32::from_le_bytes(key_and_record_size[4..].try_into().unwrap()) as usize;

        let mut key_buf = vec![0; key_size];
        let mut record_buf = vec![0; record_size];

        self.reader
            .read_exact(&mut key_buf)
            .map_err(|_| vm::Error::Corrupted)?;

        self.reader
            .read_exact(&mut record_buf)
            .map_err(|_| vm::Error::Corrupted)?;

        Ok(Some(MergeEntry::from(
            Record::new(key_buf),
            Record::new(record_buf),
            0,
        )))
    }
}

struct MergeState {
    readers: Vec<SortRunReader>,
    heap: BinaryHeap<MergeEntry, MergeEntryCmp>,
}

#[derive(Debug, Clone)]
struct MergeEntry {
    key: Record,
    record: Record,
    run_idx: usize,
}

impl MergeEntry {
    fn new(
        schema: &Schema,
        expressions: &[Expression],
        record: Record,
        run_idx: usize,
    ) -> vm::Result<Self> {
        let mut key = RecordMut::new();

        for expr in expressions {
            key.add(vm::expression::resolve_expression_to_value(
                &record, schema, expr,
            )?);
        }

        Ok(Self {
            key: key.serialize_to_record(),
            record,
            run_idx,
        })
    }

    fn from(key: Record, record: Record, run_idx: usize) -> Self {
        Self {
            key,
            record,
            run_idx,
        }
    }

    fn size(&self) -> usize {
        self.key.size() + self.record.size()
    }
}
