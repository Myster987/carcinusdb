use std::mem;

use hashbrown::HashSet;

use crate::{
    database::WriteDbTx,
    sql::{parser::statement::Expression, record::RecordMut, schema::Schema, types::Value},
    storage::{
        btree::{BTreeCursor, BTreeKey, InsertOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub fn plan_insert<'tx, DbTx: WriteDbTx + 'tx>(
    tx: &'tx DbTx,
    into: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&into)?;

    Ok(Box::new(Insert::new(
        tx.write_cursor(table.root),
        table.schema,
        columns,
        values,
    )))
}

pub struct Insert<'tx, Tx: WriteTx> {
    cursor: BTreeCursor<'tx, Tx>,
    schema: Schema,
    indices: HashSet<usize>,
    values: Vec<Vec<Expression>>,
    current: usize,
    temp_record: RecordMut,
}

impl<'tx, Tx: WriteTx> Insert<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Self {
        let indices = columns
            .iter()
            .map(|col| schema.index_of(col).unwrap())
            .collect();

        Self {
            cursor,
            schema,
            indices,
            values,
            current: 0,
            temp_record: RecordMut::new(),
        }
    }
}

impl<'tx, Tx: WriteTx> Operator for Insert<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        if self.current >= self.values.len() {
            return Ok(None);
        }

        let mut row = mem::take(&mut self.values[self.current]);
        self.current += 1;

        let row_id = row
            .first()
            .map(|expr| match expr {
                Expression::Value(val) => val.as_ref().to_int(),
                _ => panic!("expected value"),
            })
            .unwrap();

        let mut pop_record = 0;
        for idx in 0..self.schema.len() {
            if self.indices.contains(&idx) {
                let expr = mem::replace(&mut row[pop_record], Expression::Wildcard);
                let Expression::Value(value) = expr else {
                    return Err(vm::Error::Unsupported(expr));
                };
                pop_record += 1;
                self.temp_record.add(value);
            } else {
                self.temp_record.add(Value::Null);
            }
        }

        let record = self.temp_record.serialize_to_record();
        self.temp_record.clear();

        self.cursor
            .insert(
                BTreeKey::new_table_key(row_id, Some(record)),
                InsertOptions::default(),
            )
            .map_err(Into::into)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
