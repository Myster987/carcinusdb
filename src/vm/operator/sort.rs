use std::{fs::File, path::PathBuf};

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
