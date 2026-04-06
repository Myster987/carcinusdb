use crate::{
    database::DatabaseTransaction,
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

pub fn plan<'tx>(
    statement: Statement,
    tx: &DatabaseTransaction<'tx>,
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
            returning,
        } => {
            let insert = dml::insert::plan_insert(tx, into, columns, values, returning)?;
            Ok(ExecutionPlan::Query(insert))
        }
        Statement::Update {
            table,
            columns,
            r#where,
            returning,
        } => {
            let update = dml::update::plan_update(tx, table, columns, r#where, returning)?;
            Ok(ExecutionPlan::Query(update))
        }
        Statement::Delete {
            from,
            r#where,
            returning,
        } => {
            let delete = dml::delete::plan_delete(tx, from, r#where, returning)?;
            Ok(ExecutionPlan::Query(delete))
        }

        Statement::Create(s) => Ok(ExecutionPlan::Create(s)),
        Statement::Drop(s) => Ok(ExecutionPlan::Drop(s)),

        Statement::BeginTransaction => Ok(ExecutionPlan::BeginTransaction),
        Statement::Rollback => Ok(ExecutionPlan::Rollback),
        Statement::Commit => Ok(ExecutionPlan::Commit),

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan(*inner, tx)?))),
    }
}
