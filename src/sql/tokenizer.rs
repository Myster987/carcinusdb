use std::{iter::Peekable, str::Chars};

use crate::sql::{
    Error, SqlResult,
    token::{Keyword, Token, Whitespace},
};

pub struct Stream<'a> {
    input: &'a str,
    position: usize,
    chars: Peekable<Chars<'a>>,
}

impl<'a> Stream<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            position: 0,
            chars: input.chars().peekable(),
        }
    }

    pub fn raw(&self) -> &'a str {
        self.input
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn next(&mut self) -> Option<char> {
        self.chars.next().inspect(|_| self.position += 1)
    }

    pub fn peek(&mut self) -> Option<&char> {
        self.chars.peek()
    }

    pub fn peek_next(&mut self) -> Option<&char> {
        self.next();
        self.peek()
    }
}

pub struct Tokenizer<'a> {
    stream: Stream<'a>,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            stream: Stream::new(input),
        }
    }

    /// Consumes character and returns next `Token`.
    pub fn consume(&mut self, token: Token) -> SqlResult<Token> {
        self.stream.next();
        Ok(token)
    }

    pub fn consume_number(&mut self) -> SqlResult<Token> {
        let mut number = String::new();

        while let Some(digit) = self.stream.peek() {
            if digit.is_numeric() {
                number.push(self.stream.next().unwrap());
            } else {
                break;
            }
        }

        Ok(Token::Number(number))
    }

    pub fn consume_string(&mut self) -> SqlResult<Token> {
        let quote = self.stream.next().unwrap();
        let mut string = String::new();

        while let Some(ch) = self.stream.peek() {
            if *ch != quote {
                string.push(self.stream.next().unwrap());
            } else {
                break;
            }
        }

        if self.stream.next().is_some_and(|ch| ch == quote) {
            Ok(Token::String(string))
        } else {
            Err(Error::StringNotClosed)
        }
    }

    pub fn consume_keyword_or_identifier(&mut self) -> SqlResult<Token> {
        let mut to_consume = String::new();

        while let Some(ch) = self.stream.peek() {
            if Token::is_part_keyword_or_identifier(ch) {
                to_consume.push(self.stream.next().unwrap());
            } else {
                break;
            }
        }

        let keyword = match to_consume.to_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "CREATE" => Keyword::Create,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "SET" => Keyword::Set,
            "DROP" => Keyword::Drop,
            "FROM" => Keyword::From,
            "WHERE" => Keyword::Where,
            "AND" => Keyword::And,
            "OR" => Keyword::Or,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UNIQUE" => Keyword::Unique,
            "TABLE" => Keyword::Table,
            "DATABASE" => Keyword::Database,
            "INT" => Keyword::Int,
            "BIGINT" => Keyword::BigInt,
            "UNSIGNED" => Keyword::Unsigned,
            "VARCHAR" => Keyword::Varchar,
            "BOOL" => Keyword::Bool,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "INDEX" => Keyword::Index,
            "ON" => Keyword::On,
            "BEGIN" => Keyword::Begin,
            "TRANSACTION" => Keyword::Transaction,
            "ROLLBACK" => Keyword::Rollback,
            "COMMIT" => Keyword::Rollback,
            "EXPLAIN" => Keyword::Explain,
            _ => Keyword::None,
        };

        if matches!(keyword, Keyword::None) {
            Ok(Token::Identifier(to_consume))
        } else {
            Ok(Token::Keyword(keyword))
        }
    }

    pub fn next_token(&mut self) -> SqlResult<Token> {
        let Some(ch) = self.stream.peek() else {
            return Ok(Token::Eof);
        };

        match ch {
            ' ' => self.consume(Token::Whitespace(Whitespace::Space)),
            '\t' => self.consume(Token::Whitespace(Whitespace::Tab)),
            '\n' => self.consume(Token::Whitespace(Whitespace::Newline)),
            '>' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::GtEq),
                _ => Ok(Token::Gt),
            },
            '<' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::LtEq),
                _ => Ok(Token::Lt),
            },
            '=' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::Eq),
                _ => Ok(Token::Eq),
            },
            '!' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::Neq),
                _ => return Err(Error::InvalidQuery(self.stream.position())),
            },
            '+' => self.consume(Token::Add),
            '-' => self.consume(Token::Sub),
            '*' => self.consume(Token::Mul),
            '/' => self.consume(Token::Div),
            '(' => self.consume(Token::LeftParen),
            ')' => self.consume(Token::RightParen),
            ',' => self.consume(Token::Comma),
            ';' => self.consume(Token::SemiColon),
            '0'..='9' => self.consume_number(),
            '"' | '\'' => self.consume_string(),
            _ if Token::is_part_keyword_or_identifier(ch) => self.consume_keyword_or_identifier(),
            _ => Err(Error::UnexpectedToken(ch.to_owned())),
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = SqlResult<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_token = self.next_token();

        match next_token {
            Ok(Token::Eof) => None,
            Err(err) => {
                self.stream.next();
                Some(Err(err))
            }
            _ => Some(next_token), 
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::refine;

    use super::*;

    #[test]
    fn test_tokenize() -> anyhow::Result<()> {
        let stream = "SELECT col From test;";

        let tokenizer = Tokenizer::new(stream);

        let tokens: Vec<Token> = tokenizer.into_iter().map(|t| t.unwrap()).collect();

        let tokens = refine(tokens);

        println!("{:?}", tokens);

        Ok(())
    }
}
