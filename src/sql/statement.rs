/// SQL statement
#[derive(Debug)]
pub enum Statement {
    Select {
        columns: Vec<Expression>,
        from: String,
        r#where: Option<Expression>,
        order_by: Option<Vec<Expression>>,
    },
    Insert,
    Update,
    Delete,
}

/// Used in select, insert, update, delete.
#[derive(Debug)]
pub enum Expression {
    Identifier(String),

    Value(Value),

    Wildcard,

    BinaryOperation {
        left: Box<Expression>,
        operator: BinaryOperator,
        rigth: Box<Expression>,
    },
    UnaryOperation {
        operator: UnaryOperator,
        expr: Box<Expression>,
    },
    Nested(Box<Expression>),
}

#[derive(Debug)]
pub enum Value {
    String(String),
    Bool(bool),

    Number(i128),
}

#[derive(Debug)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

#[derive(Debug)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

/// SQL data types
#[derive(Debug)]
pub enum DataType {
    SmallInt,
    Int,
    BigInt,
    Boolean,
    VarChar(usize),
}
