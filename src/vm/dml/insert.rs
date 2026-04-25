use std::{collections::VecDeque, mem};

use hashbrown::HashSet;

use crate::{
    database::DatabaseTransaction,
    sql::{
        parser::statement::Expression,
        record::{Record, RecordBuilder, RecordMut},
        schema::{IndexMetadata, Schema},
        types::Value,
    },
    storage::btree::{BTreeCursor, BTreeKey, InsertOptions},
    vm::{
        self,
        operator::{Operator, Row, projection::Projection},
    },
};

pub fn plan_insert<'tx>(
    tx: &'tx DatabaseTransaction,
    into: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
    returning: Option<Vec<Expression>>,
) -> vm::Result<Insert<'tx>> {
    let is_returning = returning.is_some();
    let values = VecDeque::from(values);
    let table = tx.catalog().get_table(&into)?;

    let index_cursors = table
        .indexes
        .iter()
        .map(|idx| (idx.clone(), tx.cursor(idx.root)))
        .collect();

    let mut plan: Box<dyn Operator + 'tx> = Box::new(InsertInner::new(
        tx.cursor(table.root),
        table.schema,
        columns,
        values,
        index_cursors,
        is_returning,
    ));

    if let Some(returning) = returning {
        plan = Box::new(Projection::new(plan, returning)?);
    };

    Ok(Insert {
        child: plan,
        is_returning,
    })
}

pub struct Insert<'tx> {
    child: Box<dyn Operator + 'tx>,
    pub is_returning: bool,
}

impl<'tx> Operator for Insert<'tx> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next(&mut self) -> vm::Result<Option<Row>> {
        self.child.next()
    }
}

struct InsertInner<'tx> {
    cursor: BTreeCursor<'tx>,
    schema: Schema,
    indices: HashSet<usize>,
    values: VecDeque<Vec<Expression>>,
    temp_record: RecordMut,
    index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx>)>,
    insert_options: InsertOptions,
}

impl<'tx> InsertInner<'tx> {
    pub fn new(
        cursor: BTreeCursor<'tx>,
        schema: Schema,
        columns: Vec<String>,
        values: VecDeque<Vec<Expression>>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx>)>,
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
            temp_record: RecordMut::new(),
            index_cursors,
            insert_options,
        }
    }
}

impl<'tx> Operator for InsertInner<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        let Some(mut row) = self.values.pop_front() else {
            return Ok(None);
        };

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

        Ok(inserted_record.or(Some(Record::empty())))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
