use crate::sql::{statement::Statement, token::Token};

/// Removes all whitespaces from token stream.
pub fn refine(token_stream: Vec<Token>) -> Vec<Token> {
    token_stream.into_iter().filter(|t| !matches!(t, Token::Whitespace(_)) ).collect()
}


pub fn parse(token_stream: Vec<Token>) -> Statement {
    

    todo!()
}