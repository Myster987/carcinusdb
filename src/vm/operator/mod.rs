use crate::{
    sql::{record::Record, schema::Schema},
    vm,
};

pub mod collect;
pub mod filter;
pub mod index_scan;
pub mod projection;
pub mod seq_scan;

pub trait Operator {
    fn next(&mut self) -> vm::Result<Option<Record>>;
    fn schema(&self) -> &Schema;
}
