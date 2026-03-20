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
            let select = dml::select::plan_select(tx, columns, from, r#where, order_by)?;
            Ok(ExecutionPlan::Query(select))
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
            let select = dml::select::plan_select(tx, columns, from, r#where, order_by)?;
            Ok(ExecutionPlan::Query(select))
        }
        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let insert = dml::insert::plan_insert(tx, into, columns, values)?;
            Ok(ExecutionPlan::Query(insert))
        }
        Statement::Update {
            table,
            columns,
            r#where,
        } => {
            let update = dml::update::plan_update(tx, table, columns, r#where)?;
            Ok(ExecutionPlan::Query(update))
        }
        Statement::Delete { from, r#where } => {
            let delete = dml::delete::plan_delete(tx, from, r#where)?;
            Ok(ExecutionPlan::Query(delete))
        }

        Statement::Create(s) => Ok(ExecutionPlan::Create(s)),
        Statement::Drop(s) => Ok(ExecutionPlan::Drop(s)),

        Statement::BeginTransaction => Ok(ExecutionPlan::BeginTransaction),
        Statement::Rollback => Ok(ExecutionPlan::Rollback),
        Statement::Commit => Ok(ExecutionPlan::Commit),

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan_write(*inner, tx)?))),
    }
}
