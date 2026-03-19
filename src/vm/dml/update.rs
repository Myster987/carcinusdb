use std::collections::HashMap;

use crate::{
    sql::{
        parser::statement::{Assignment, Expression},
        record::RecordMut,
        schema::Schema,
        types::Value,
    },
    storage::{
        btree::{BTreeCursor, BTreeKey, DatabaseCursor, InsertOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        expression::resolve_expression_to_value,
        operator::{Operator, Row},
    },
};

pub struct Update<'tx, Tx: WriteTx> {
    cursor: BTreeCursor<'tx, Tx>,
    schema: Schema,
    assignments: HashMap<usize, Value>,
    predicate: Option<Expression>,
    started: bool,
}

impl<'tx, Tx: WriteTx> Update<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        assignments: Vec<Assignment>,
        predicate: Option<Expression>,
    ) -> vm::Result<Self> {
        let mut assignment_map = HashMap::with_capacity(assignments.len());

        for Assignment { identifier, value } in assignments {
            let index = schema.index_of(&identifier).unwrap();
            let Expression::Value(value) = value else {
                panic!()
            };
            assignment_map.insert(index, value);
        }

        Ok(Self {
            cursor,
            schema,
            assignments: assignment_map,
            predicate,
            started: false,
        })
    }
}

impl<'tx, Tx: WriteTx> Operator for Update<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        if !self.started {
            self.started = true;
            if !self.cursor.seek_first()? {
                return Ok(None);
            }
        }

        loop {
            let row = match self.cursor.try_record() {
                Ok(row) => row,
                Err(_) => return Ok(None),
            };

            let matches = match &self.predicate {
                None => true,
                Some(expr) => matches!(
                    resolve_expression_to_value(&row, &self.schema, expr)?,
                    Value::Bool(true)
                ),
            };

            if matches {
                let mut record_builder = RecordMut::from_record(&row);

                for (i, assaign) in self.assignments.iter() {
                    let _ = record_builder.set(*i, assaign.clone());
                }

                let row_id = row.get_value(0).to_int();
                let new_record = record_builder.serialize_to_record();

                let inserted_record = self.cursor.update_current(
                    BTreeKey::new_table_key(row_id, Some(new_record)),
                    InsertOptions::default(),
                )?;

                return Ok(inserted_record);
            } else {
                if !self.cursor.next()? {
                    return Ok(None);
                }
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
