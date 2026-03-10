use crate::{
    database::{CARCINUSDB_MASTER_TABLE_ROOT, WriteDbTx},
    sql::{
        parser::{
            self,
            statement::{Constrains, Create},
        },
        record::RecordBuilder,
        schema::{Schema, TableMetadata},
        types::{Value, text::Text},
    },
    storage::btree::{BTreeKey, BTreeType, InsertOptionsBuilder},
    vm::{self, query_result::QueryResult},
};

pub fn create<'tx, Tx: WriteDbTx>(tx: &'tx Tx, statement: Create) -> vm::Result<QueryResult<'tx>> {
    match statement {
        Create::Table { name, columns } => create_table(tx, name, columns),
        Create::Index {
            name,
            table,
            column,
            unique,
        } => todo!(),
        _ => todo!(),
    }
}

fn create_table<'tx, Tx: WriteDbTx>(
    tx: &'tx Tx,
    name: String,
    columns: Vec<parser::statement::Column>,
) -> vm::Result<QueryResult<'tx>> {
    let root_page = tx.create_btree(BTreeType::Table)?;

    // 2. reconstruct the original sql to store in master table
    let sql = reconstruct_create_table_sql(&name, &columns);

    let mut record = RecordBuilder::new();

    record.add(Value::Text(Text::new("table".into()))); // type
    record.add(Value::Text(Text::new(name.clone()))); // name
    record.add(Value::Text(Text::new(name.clone()))); // tbl_name
    record.add(Value::Int(root_page as i64)); // root_page
    record.add(Value::Text(Text::new(sql))); // sql

    let record = record.serialize_to_record();

    let mut cursor = tx.write_cursor(CARCINUSDB_MASTER_TABLE_ROOT);

    let row_id = cursor.next_row_id()?;

    cursor.insert(
        BTreeKey::new_table_key(row_id, Some(record)),
        InsertOptionsBuilder::new().build(),
    )?;

    let schema = Schema::new(columns.into_iter().map(|c| c.into()).collect());

    tx.catalog().insert_table(
        name.clone(),
        TableMetadata::new(root_page, name, schema, vec![]),
    );

    Ok(QueryResult::RowsAffected(1))
}

// reconstructs "CREATE TABLE name (col1 TYPE, col2 TYPE NOT NULL, ...)"
fn reconstruct_create_table_sql(name: &str, columns: &[parser::statement::Column]) -> String {
    let cols = columns
        .iter()
        .map(|col| {
            let mut s = format!("{} {}", col.name, col.data_type);
            for constraint in &col.constrains {
                match constraint {
                    Constrains::PrimaryKey => s.push_str(" PRIMARY KEY"),
                    Constrains::NotNull => s.push_str(" NOT NULL"),
                    Constrains::Unique => s.push_str(" UNIQUE"),
                }
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!("CREATE TABLE {name} ({cols})")
}
