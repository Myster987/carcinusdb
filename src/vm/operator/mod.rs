use crate::{
    sql::{record::Record, schema::Schema},
    vm,
};

pub mod filter;
pub mod projection;
pub mod seq_scan;

pub type Row = Record<'static>;

pub trait Operator {
    fn next(&mut self) -> vm::Result<Option<Row>>;
    fn schema(&self) -> &Schema;
}
