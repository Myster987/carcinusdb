use std::{cmp::Reverse, collections::BinaryHeap, fs::File, path::PathBuf};

use crate::{
    sql::{parser::statement::Expression, record::Record},
    storage::page::Page,
    utils::io::BlockIO,
    vm::{
        self,
        operator::{Operator, collect::Collect},
    },
};

pub struct Sort<'tx> {
    sorted: bool,

    order_by: Vec<Expression>,
    collect: Collect<'tx>,
    record_buffer: Page,

    temp_dir: PathBuf,
    input_file: Option<BlockIO<File>>,
    output_file: Option<BlockIO<File>>,
}

impl<'tx> Sort<'tx> {
    pub fn new(
        order_by: Vec<Expression>,
        collect: Collect<'tx>,
        temp_dir: PathBuf,
        page_size: usize,
    ) -> Self {
        Self {
            sorted: false,
            order_by,
            collect,
            temp_dir,
            record_buffer: Page::alloc(page_size, None),
            input_file: None,
            output_file: None,
        }
    }
}

impl<'tx> Operator for Sort<'tx> {
    fn schema(&self) -> &crate::sql::schema::Schema {
        self.collect.schema()
    }

    fn next(&mut self) -> vm::Result<Option<Record>> {
        todo!()
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

    fn load_to_max_size(&mut self, record_src: &mut Collect<'_>) -> vm::Result<()> {
        // TODO: implement some kind of peek for Collect.

        Ok(())
    }

    fn can_fit(&self, record: &Record) -> bool {
        self.current_size + record.size() <= self.max_size
    }

    fn pop(&mut self) -> Option<Record> {
        let popped = self.heap.pop().map(|Reverse(r)| r);
        self.last_pop = popped.clone();
        popped
    }

    fn push(&mut self, new_record: Record) {
        if let Some(last_popped) = &self.last_pop {
            if &new_record < last_popped {
                self.freeze_list.push(new_record);
            } else {
                self.heap.push(Reverse(new_record));
            }
        }
    }
}
