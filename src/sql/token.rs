use std::fmt::{self, Display, Write};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    Identifier(String),
    Whitespace(Whitespace),
    String(String),
    Number(String),
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
    LeftParen,
    RightParen,
    Comma,
    SemiColon,
    /// Only indicates end of token stream
    Eof,
}

impl Token {
    pub fn is_part_keyword_or_identifier(ch: &char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || *ch == '_'
    }
}

impl From<&Keyword> for Token {
    fn from(value: &Keyword) -> Self {
        Token::Keyword(*value)
    }
}
impl Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Eof => f.write_str("EOF"),
            Self::Whitespace(whitespace) => write!(f, "{whitespace}"),
            Self::Keyword(keyword) => write!(f, "{keyword}"),
            Self::Identifier(identifier) => f.write_str(identifier),
            Self::String(string) => write!(f, "\"{string}\""),
            Self::Number(number) => write!(f, "{number}"),
            Self::Eq => f.write_str("="),
            Self::Neq => f.write_str("!="),
            Self::Lt => f.write_str("<"),
            Self::Gt => f.write_str(">"),
            Self::LtEq => f.write_str("<="),
            Self::GtEq => f.write_str(">="),
            Self::Mul => f.write_str("*"),
            Self::Div => f.write_str("/"),
            Self::Add => f.write_str("+"),
            Self::Sub => f.write_str("-"),
            Self::LeftParen => f.write_str("("),
            Self::RightParen => f.write_str(")"),
            Self::Comma => f.write_str(","),
            Self::SemiColon => f.write_str(";"),
        }
    }
}

/// SQL keywords.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Keyword {
    Select,
    Create,
    Update,
    Delete,
    Insert,
    Into,
    Values,
    Set,
    Drop,
    From,
    Where,
    And,
    Or,
    Primary,
    Key,
    Unique,
    Table,
    Database,
    Int,
    BigInt,
    Unsigned,
    Varchar,
    Bool,
    True,
    False,
    Order,
    By,
    Index,
    On,
    Begin,
    Transaction,
    Rollback,
    Commit,
    Explain,
    /// Not used, only for convinience
    None,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Select => "SELECT",
            Self::Create => "CREATE",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::Values => "VALUES",
            Self::Set => "SET",
            Self::Drop => "DROP",
            Self::From => "FROM",
            Self::Where => "WHERE",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Primary => "PRIMARY",
            Self::Key => "KEY",
            Self::Unique => "UNIQUE",
            Self::Table => "TABLE",
            Self::Database => "DATABASE",
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Unsigned => "UNSIGNED",
            Self::Varchar => "VARCHAR",
            Self::Bool => "BOOL",
            Self::True => "TRUE",
            Self::False => "FALSE",
            Self::Order => "ORDER",
            Self::By => "BY",
            Self::Index => "INDEX",
            Self::On => "ON",
            Self::Begin => "BEGIN",
            Self::Transaction => "TRANSACTION",
            Self::Rollback => "ROLLBACK",
            Self::Commit => "COMMIT",
            Self::Explain => "EXPLAIN",
            Self::None => "NONE",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Whitespace {
    Space,
    Tab,
    Newline,
}

impl Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(match self {
            Self::Space => ' ',
            Self::Tab => '\t',
            Self::Newline => '\n',
        })
    }
}
