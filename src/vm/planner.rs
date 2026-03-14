use crate::{
    database::{ReadDbTx, WriteDbTx},
    sql::parser::statement::{Create, Drop, Statement},
    vm::{self, dml, operator::Operator},
};

pub enum ExecutionPlan<'tx> {
    // iterator based
    Query(Box<dyn Operator + 'tx>),

    // table or index
    Create(Create),
    Drop(Drop),

    // transaction control
    BeginTransaction,
    Rollback,
    Commit,

    // meta
    Explain(Box<ExecutionPlan<'tx>>),
}

pub fn plan_read<'tx, Tx: ReadDbTx>(
    statement: Statement,
    tx: &'tx Tx,
) -> vm::Result<ExecutionPlan<'tx>> {
    match statement {
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => {
            let select = dml::select::Select::new(tx, columns, from, r#where, order_by)?;
            Ok(ExecutionPlan::Query(Box::new(select)))
        }

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan_read(*inner, tx)?))),

        _ => Err(vm::Error::ReadOnly),
    }
}

pub fn plan_write<'tx>(
    statement: Statement,
    tx: &'tx impl WriteDbTx,
) -> vm::Result<ExecutionPlan<'tx>> {
    match statement {
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => {
            let select = dml::select::Select::new(tx, columns, from, r#where, order_by)?;
            Ok(ExecutionPlan::Query(Box::new(select)))
        }
        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let table = tx.catalog().get_table(&into)?;
            let insert = dml::insert::Insert::new(
                tx.write_cursor(table.root),
                table.schema,
                columns,
                values,
            );
            Ok(ExecutionPlan::Query(Box::new(insert)))
        }
        Statement::Update {
            table,
            columns,
            r#where,
        } => {
            let table = tx.catalog().get_table(&table)?;
            let update = dml::update::Update::new(
                tx.write_cursor(table.root),
                table.schema,
                columns,
                r#where,
            )?;
            Ok(ExecutionPlan::Query(Box::new(update)))
        }
        Statement::Delete { from, r#where } => {
            let table = tx.catalog().get_table(&from)?;
            let delete =
                dml::delete::Delete::new(tx.write_cursor(table.root), table.schema, r#where)?;
            Ok(ExecutionPlan::Query(Box::new(delete)))
        }

        Statement::Create(s) => Ok(ExecutionPlan::Create(s)),
        Statement::Drop(s) => Ok(ExecutionPlan::Drop(s)),

        Statement::BeginTransaction => Ok(ExecutionPlan::BeginTransaction),
        Statement::Rollback => Ok(ExecutionPlan::Rollback),
        Statement::Commit => Ok(ExecutionPlan::Commit),

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan_write(*inner, tx)?))),
    }
}
