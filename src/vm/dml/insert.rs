use std::mem;

use hashbrown::HashSet;

use crate::{
    database::WriteDbTx,
    sql::{
        parser::statement::Expression,
        record::{RecordBuilder, RecordMut},
        schema::{IndexMetadata, Schema},
        types::Value,
    },
    storage::{
        btree::{BTreeCursor, BTreeKey, InsertOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        operator::{Operator, Row, projection::Projection},
    },
};

pub fn plan_insert<'tx, DbTx: WriteDbTx + 'tx>(
    tx: &'tx DbTx,
    into: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
    returning: Option<Vec<Expression>>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&into)?;

    let index_cursors = table
        .indexes
        .iter()
        .map(|idx| (idx.clone(), tx.write_cursor(idx.root)))
        .collect();

    let mut plan: Box<dyn Operator + 'tx> = Box::new(Insert::new(
        tx.write_cursor(table.root),
        table.schema,
        columns,
        values,
        index_cursors,
        returning.is_some(),
    ));

    if let Some(returning) = returning {
        plan = Box::new(Projection::new(plan, returning)?);
    };

    Ok(plan)
}

pub struct Insert<'tx, Tx: WriteTx> {
    cursor: BTreeCursor<'tx, Tx>,
    schema: Schema,
    indices: HashSet<usize>,
    values: Vec<Vec<Expression>>,
    current: usize,
    temp_record: RecordMut,
    index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
    insert_options: InsertOptions,
}

impl<'tx, Tx: WriteTx> Insert<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
        is_returning: bool,
    ) -> Self {
        let indices = columns
            .iter()
            .map(|col| schema.index_of(col).unwrap())
            .collect();

        let mut insert_options = InsertOptions::default();

        if is_returning {
            insert_options.set_returning();
        }

        Self {
            cursor,
            schema,
            indices,
            values,
            current: 0,
            temp_record: RecordMut::new(),
            index_cursors,
            insert_options,
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

        let inserted_record = self.cursor.insert(
            BTreeKey::new_table_key(row_id, Some(record.to_owned())),
            self.insert_options,
        )?;

        for (index_metadata, index_cursor) in self.index_cursors.iter_mut() {
            let col_idx = index_metadata.column_index;
            let col_value = record.get_value(col_idx).to_owned();
            let row_id = record.get_value(0).to_int();

            let record = RecordBuilder::new()
                .add(col_value)
                .add(Value::Int(row_id))
                .build();

            let key = BTreeKey::new_index_key(record);
            index_cursor.insert(key, InsertOptions::default())?;
        }

        Ok(inserted_record)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
