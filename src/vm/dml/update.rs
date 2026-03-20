use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    database::WriteDbTx,
    sql::{
        parser::statement::{Assignment, Expression},
        record::RecordMut,
        schema::{IndexMetadata, Schema},
        types::Value,
    },
    storage::{
        btree::{BTreeCursor, BTreeKey, InsertOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        operator::{
            Operator, Row,
            filter::Filter,
            index_scan::{IndexScan, find_index},
            seq_scan::SeqScan,
        },
    },
};

pub fn plan_update<'tx, DbTx: WriteDbTx + 'tx>(
    tx: &'tx DbTx,
    table: String,
    columns: Vec<Assignment>,
    r#where: Option<Expression>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&table)?;

    match find_index(&r#where, &table) {
        Some((index, kind, filter)) => {
            let table_cursor = Rc::new(RefCell::new(tx.write_cursor(table.root)));
            let index_scan = IndexScan::new(
                Rc::new(RefCell::new(tx.read_cursor(index.root))),
                table_cursor.clone(),
                kind,
                table.schema.clone(),
            );
            let source: Box<dyn Operator + 'tx> = match filter {
                Some(expr) => Box::new(Filter::new(Box::new(index_scan), expr)),
                None => Box::new(index_scan),
            };

            let index_cursors = table
                .indexes
                .iter()
                .map(|idx| (idx.clone(), tx.write_cursor(idx.root)))
                .collect();

            Ok(Box::new(Update::with_index_source(
                table_cursor,
                source,
                table.schema,
                columns,
                index_cursors,
            )))
        }
        // no matching index found. fallback to sequential scan. still corect,
        // just slower.
        None => {
            let index_cursors = table
                .indexes
                .iter()
                .map(|idx| (idx.clone(), tx.write_cursor(idx.root)))
                .collect();

            Ok(Box::new(Update::sequential_scan(
                tx.write_cursor(table.root),
                table.schema.clone(),
                columns,
                r#where,
                index_cursors,
            )?))
        }
    }
}

pub struct Update<'tx, Tx: WriteTx> {
    cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    source: Box<dyn Operator + 'tx>,
    assignments: HashMap<usize, Value>,
    index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
}

impl<'tx, Tx: WriteTx + 'tx> Update<'tx, Tx> {
    // pub fn new(
    //     cursor: BTreeCursor<'tx, Tx>,
    //     schema: Schema,
    //     assignments: Vec<Assignment>,
    //     predicate: Option<Expression>,
    // ) -> Self {
    //     let mut assignment_map = HashMap::with_capacity(assignments.len());

    //     for Assignment { identifier, value } in assignments {
    //         let index = schema.index_of(&identifier).unwrap();
    //         let Expression::Value(value) = value else {
    //             panic!()
    //         };
    //         assignment_map.insert(index, value);
    //     }

    //     Self {
    //         cursor,
    //         schema,
    //         assignments: assignment_map,
    //         predicate,
    //         started: false,
    //     }
    // }

    pub fn sequential_scan(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        assignments: Vec<Assignment>,
        r#where: Option<Expression>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
    ) -> vm::Result<Self> {
        let mut assignment_map = HashMap::with_capacity(assignments.len());

        for Assignment { identifier, value } in assignments {
            let index = schema.index_of(&identifier).unwrap();
            let Expression::Value(value) = value else {
                panic!()
            };
            assignment_map.insert(index, value);
        }

        let cursor = Rc::new(RefCell::new(cursor));
        let scan = SeqScan::new(cursor.clone(), schema);

        let mut source: Box<dyn Operator + 'tx> = Box::new(scan);

        if let Some(expr) = r#where {
            source = Box::new(Filter::new(source, expr));
        }

        Ok(Self {
            cursor,
            source,
            assignments: assignment_map,
            index_cursors,
        })
    }

    pub fn with_index_source(
        cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        source: Box<dyn Operator + 'tx>,
        schema: Schema,
        assignments: Vec<Assignment>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
    ) -> Self {
        let mut assignment_map = HashMap::with_capacity(assignments.len());

        for Assignment { identifier, value } in assignments {
            let index = schema.index_of(&identifier).unwrap();
            let Expression::Value(value) = value else {
                panic!()
            };
            assignment_map.insert(index, value);
        }

        Self {
            cursor,
            source,
            assignments: assignment_map,
            index_cursors,
        }
    }
}

impl<'tx, Tx: WriteTx> Operator for Update<'tx, Tx> {
    // fn next(&mut self) -> vm::Result<Option<Row>> {
    //     if !self.started {
    //         self.started = true;
    //         if !self.cursor.seek_first()? {
    //             return Ok(None);
    //         }
    //     }

    //     loop {
    //         let row = match self.cursor.try_record() {
    //             Ok(row) => row,
    //             Err(_) => return Ok(None),
    //         };

    //         let matches = match &self.predicate {
    //             None => true,
    //             Some(expr) => matches!(
    //                 resolve_expression_to_value(&row, &self.schema, expr)?,
    //                 Value::Bool(true)
    //             ),
    //         };

    //         if matches {
    //             let mut record_builder = RecordMut::from_record(&row);

    //             for (i, assaign) in self.assignments.iter() {
    //                 let _ = record_builder.set(*i, assaign.clone());
    //             }

    //             let row_id = row.get_value(0).to_int();
    //             let new_record = record_builder.serialize_to_record();

    //             let inserted_record = self.cursor.update_current(
    //                 BTreeKey::new_table_key(row_id, Some(new_record)),
    //                 InsertOptions::default(),
    //             )?;

    //             return Ok(inserted_record);
    //         } else {
    //             if !self.cursor.next()? {
    //                 return Ok(None);
    //             }
    //         }
    //     }
    // }

    fn next(&mut self) -> vm::Result<Option<Row>> {
        let Some(row) = self.source.next()? else {
            return Ok(None);
        };

        let mut record_builder = RecordMut::from_record(&row);

        for (i, assaign) in self.assignments.iter() {
            let _ = record_builder.set(*i, assaign.clone());
        }

        let row_id = row.get_value(0).to_int();
        let new_record = record_builder.serialize_to_record();

        let inserted_record = self.cursor.borrow_mut().update_current(
            BTreeKey::new_table_key(row_id, Some(new_record)),
            InsertOptions::default(),
        )?;

        return Ok(inserted_record);
    }

    fn schema(&self) -> &Schema {
        self.source.schema()
    }
}
