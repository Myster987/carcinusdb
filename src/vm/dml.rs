use std::mem;

use crate::{
    database::WriteDbTx,
    sql::{parser::statement::Expression, record::RecordBuilder, types::Value},
    storage::btree::{BTreeKey, InsertOptions},
    vm::{self, query_result::QueryResult},
};

pub fn insert<'tx, Tx: WriteDbTx>(
    tx: &'tx Tx,
    into: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
) -> vm::Result<QueryResult<'tx>> {
    let table = tx.catalog().get_table(&into)?;
    let indicies: hashbrown::HashSet<_> = columns
        .iter()
        .map(|col| table.index_of(col).unwrap())
        .collect();

    let mut cursor = tx.write_cursor(table.root);
    let mut record_builder = RecordBuilder::new();
    let inserted_rows = values.len();

    for mut row in values {
        let row_id = row
            .first()
            .map(|expr| match expr {
                Expression::Value(val) => val.as_ref().to_int(),
                _ => panic!(),
            })
            .unwrap();
        let mut pop_record = 0;

        for idx in 0..table.schema.len() {
            if indicies.contains(&idx) {
                let expr = mem::replace(&mut row[pop_record], Expression::Wildcard);
                let Expression::Value(value) = expr else {
                    return Err(vm::Error::Unsupported(expr));
                };
                pop_record += 1;
                record_builder.add(value);
            } else {
                record_builder.add(Value::Null);
            }
        }

        let entry = BTreeKey::new_table_key(row_id, Some(record_builder.serialize_to_record()));

        cursor.insert(entry, InsertOptions::default())?;
        record_builder.clear();
    }

    Ok(QueryResult::RowsAffected(inserted_rows))
}
