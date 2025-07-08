use libc::msghdr;

use crate::sql::{
    Error, SqlResult,
    statement::{
        Assignment, BinaryOperator, Column, Constrains, Create, DataType, Drop, Expression,
        Statement, UnaryOperator, Value,
    },
    token::{Keyword, Token},
    tokenizer::Tokenizer,
};

/// Trait to implement all esential statements.
pub trait StatementParser {
    fn parse_explain(&mut self) -> SqlResult<Statement>;
    fn parse_select(&mut self) -> SqlResult<Statement>;
    fn parse_create(&mut self) -> SqlResult<Statement>;
    fn parse_drop(&mut self) -> SqlResult<Statement>;
    fn parse_delete(&mut self) -> SqlResult<Statement>;
    fn parse_insert(&mut self) -> SqlResult<Statement>;
    fn parse_update(&mut self) -> SqlResult<Statement>;
    fn parse_begin(&mut self) -> SqlResult<Statement>;
    fn parse_commit(&mut self) -> SqlResult<Statement>;
}

pub struct Parser<'a> {
    input: &'a str,
    /// Current position of next token.
    /// - position - 2 -> previous token
    /// - position - 1 -> current token
    /// - position -> next token
    position: usize,
    tokens: Vec<Token>,
}

impl<'a> Parser<'a> {
    /// Creates new `Parser`.
    ///
    /// # Fails
    ///
    /// If any `Token` in query is invalid, then it returns `Error`.
    pub fn new(input: &'a str) -> SqlResult<Self> {
        let tokenzied = Tokenizer::new(input).into_iter().collect();
        let valid_tokens = Self::validate(tokenzied)?;
        Ok(Self {
            input,
            position: 0,
            tokens: valid_tokens,
        })
    }

    /// Converts `Vec<SqlResult<Token>>` into `SqlResult<Vec<Token>>`, so it's easier to work with.
    fn validate(token_stream: Vec<SqlResult<Token>>) -> SqlResult<Vec<Token>> {
        token_stream.into_iter().collect()
    }

    fn token_at(&self, idx: usize) -> Option<&Token> {
        self.tokens.get(idx)
    }

    fn previous(&self) -> Option<&Token> {
        self.token_at(self.position - 2)
    }

    fn current(&self) -> Option<&Token> {
        self.token_at(self.position - 1)
    }

    fn peek(&self) -> Option<&Token> {
        self.token_at(self.position)
    }

    fn next(&mut self) -> Option<Token> {
        self.token_at(self.position)
            .map(|t| t.clone())
            .inspect(|_| self.position += 1)
    }

    /// Returns back to last non-whitespace `Token`.
    fn prev_token(&mut self) {
        loop {
            assert!(self.position > 0);
            self.position -= 1;
            if let Some(Token::Whitespace(_)) = self.token_at(self.position) {
                continue;
            }
            break;
        }
    }

    fn peek_token(&mut self) -> SqlResult<&Token> {
        while let Some(token) = self.peek() {
            match token {
                Token::Whitespace(_) => self.next(),
                _ => break,
            };
        }
        self.peek().ok_or(Error::UnexpectedEof)
    }

    /// Returns next token and skips all whitespaces. Assumes that user will stop calling when `Token::Semi` is returned. Otherwise it will constantly return `Token::Eof`.
    fn next_token(&mut self) -> SqlResult<Token> {
        while let Some(token) = self.peek() {
            match token {
                Token::Whitespace(_) => self.next(),
                _ => break,
            };
        }
        self.next().ok_or(Error::UnexpectedEof)
    }

    fn next_keyword(&mut self) -> SqlResult<Keyword> {
        match self.next_token()? {
            Token::Keyword(keyword) => Ok(keyword),
            _ => Err(Error::InvalidQuery(self.position)),
        }
    }

    /// Consumes and matches next token if it matches expected one. If next token == expected it returns Ok(token). Otherwise returns error.
    fn expect_token(&mut self, expected: Token) -> SqlResult<Token> {
        match self.next_token() {
            Ok(token) => {
                if token == expected {
                    Ok(token)
                } else {
                    Err(Error::Expected {
                        expected,
                        found: token,
                    })
                }
            }
            _ => Err(Error::InvalidQuery(self.position)),
        }
    }

    /// The same as `exptected_token`, but for keywords.
    fn expect_keyword(&mut self, expected: Keyword) -> SqlResult<Keyword> {
        self.expect_token(Token::Keyword(expected))
            .map(|_| expected)
    }

    /// If `optional == next_token`, then it's consumed and function returns true. Otherwise returns false.
    fn consume_optional_token(&mut self, optional: Token) -> bool {
        match self.peek_token() {
            Ok(token) if token == &optional => {
                self.next_token().unwrap();
                true
            }
            _ => false,
        }
    }

    /// Does the same as `consume_optional_token`, but for keywords.
    fn consume_optional_keyword(&mut self, optional: Keyword) -> bool {
        self.consume_optional_token(Token::Keyword(optional))
    }

    fn consume_one_of<'k, K>(&mut self, keywords: &'k K) -> Keyword
    where
        &'k K: IntoIterator<Item = &'k Keyword>,
    {
        *keywords
            .into_iter()
            .find(|keyword| self.consume_optional_keyword(**keyword))
            .unwrap_or(&Keyword::None)
    }

    fn expect_one_of<'k, K>(&mut self, keywords: &'k K) -> SqlResult<Keyword>
    where
        &'k K: IntoIterator<Item = &'k Keyword>,
    {
        match self.consume_one_of(keywords) {
            Keyword::None => Err(Error::ExpectedOneOf {
                expected: keywords.into_iter().map(|k| Token::Keyword(*k)).collect(),
                found: Token::Keyword(Keyword::None),
            }),
            keyword => Ok(keyword),
        }
    }

    fn get_next_precedence(&mut self) -> u8 {
        let Ok(token) = self.peek_token() else {
            return 0;
        };

        match token {
            Token::Keyword(Keyword::Or) => 10,
            Token::Keyword(Keyword::And) => 20,
            Token::Eq | Token::Neq | Token::Gt | Token::GtEq | Token::Lt | Token::LtEq => 30,
            Token::Add | Token::Sub => 40,
            Token::Mul | Token::Div => 50,
            _ => 0,
        }
    }

    /// Initialized TDOP descent. This function starts recursively calling other functions.
    fn parse_expression(&mut self) -> SqlResult<Expression> {
        self.parse_expr(0)
    }

    fn parse_expr(&mut self, precedence: u8) -> SqlResult<Expression> {
        let mut expr = self.parse_prefix()?;
        let mut next_precedence = self.get_next_precedence();

        while precedence < next_precedence {
            expr = self.parse_infix(expr, next_precedence)?;
            next_precedence = self.get_next_precedence();
        }

        Ok(expr)
    }

    fn parse_prefix(&mut self) -> SqlResult<Expression> {
        match self.next_token()? {
            Token::Identifier(identifier) => Ok(Expression::Identifier(identifier)),
            Token::Mul => Ok(Expression::Wildcard),

            Token::String(value) => Ok(Expression::Value(Value::String(value))),
            Token::Keyword(Keyword::True) => Ok(Expression::Value(Value::Bool(true))),
            Token::Keyword(Keyword::False) => Ok(Expression::Value(Value::Bool(false))),
            Token::Number(num) => Ok(Expression::Value(Value::Number(
                num.parse().map_err(|_| Error::NumberOutOfRange)?,
            ))),

            token @ (Token::Add | Token::Sub) => {
                let operator = match token {
                    Token::Add => UnaryOperator::Plus,
                    Token::Sub => UnaryOperator::Minus,
                    _ => unreachable!(),
                };

                let expr = Box::new(self.parse_expr(100)?);

                Ok(Expression::UnaryOperation { operator, expr })
            }

            Token::LeftParen => {
                let expression = self.parse_expression()?;
                self.expect_token(Token::RightParen)?;
                Ok(Expression::Nested(Box::new(expression)))
            }

            _ => Err(Error::InvalidQuery(self.position)),
        }
    }

    /// Parses operations like `1 + 2` or `col == 4`. Returns `SqlResult<Expression>`.
    fn parse_infix(&mut self, left: Expression, precedence: u8) -> SqlResult<Expression> {
        let operator = match self.next_token()? {
            Token::Add => BinaryOperator::Add,
            Token::Sub => BinaryOperator::Sub,
            Token::Mul => BinaryOperator::Mul,
            Token::Div => BinaryOperator::Div,
            Token::Eq => BinaryOperator::Eq,
            Token::Neq => BinaryOperator::Neq,
            Token::Gt => BinaryOperator::Gt,
            Token::GtEq => BinaryOperator::GtEq,
            Token::Lt => BinaryOperator::Lt,
            Token::LtEq => BinaryOperator::LtEq,
            Token::Keyword(Keyword::Or) => BinaryOperator::Or,
            Token::Keyword(Keyword::And) => BinaryOperator::And,
            _ => Err(Error::InvalidQuery(self.position))?,
        };

        Ok(Expression::BinaryOperation {
            left: Box::new(left),
            operator,
            rigth: Box::new(self.parse_expr(precedence)?),
        })
    }

    /// Parses identifier (like column name is SELECT or FROM table)
    fn parse_identifier(&mut self) -> SqlResult<String> {
        self.next_token().and_then(|token| match token {
            Token::Identifier(value) => Ok(value),
            _ => Err(Error::InvalidQuery(self.position)),
        })
    }

    /// Parses with `custom_parse` function returning `Vec<T>` of values. If `required_parenthesis == true`, then it also checks if parethesis are closed correctly.
    fn parse_comma_separeted<T>(
        &mut self,
        mut custom_parser: impl FnMut(&mut Self) -> SqlResult<T>,
        required_parenthesis: bool,
    ) -> SqlResult<Vec<T>> {
        if required_parenthesis {
            self.expect_token(Token::LeftParen)?;
        }

        let mut result = vec![custom_parser(self)?];

        while let Ok(Token::Comma) = self.peek_token() {
            self.next_token()?;
            result.push(custom_parser(self)?);
        }

        if required_parenthesis {
            self.expect_token(Token::RightParen)?;
        }

        Ok(result)
    }

    ///
    fn parse_comma_separeted_values(&mut self) -> SqlResult<Vec<Expression>> {
        self.parse_comma_separeted(Self::parse_expression, false)
    }

    fn parse_identifier_list(&mut self) -> SqlResult<Vec<String>> {
        self.parse_comma_separeted(Self::parse_identifier, true)
    }

    fn parse_optional_identifier_list(&mut self) -> SqlResult<Option<Vec<String>>> {
        if self
            .peek_token()
            .is_ok_and(|token| matches!(token, Token::LeftParen))
        {
            Ok(Some(self.parse_identifier_list()?))
        } else {
            Ok(None)
        }
    }

    fn parse_assignment(&mut self) -> SqlResult<Assignment> {
        let identifier = self.parse_identifier()?;
        self.expect_token(Token::Eq)?;
        let value = self.parse_expression()?;

        Ok(Assignment { identifier, value })
    }

    fn parse_data_type(&mut self) -> SqlResult<DataType> {
        match self.next_keyword()? {
            Keyword::Int => Ok(DataType::Int),
            Keyword::BigInt => Ok(DataType::BigInt),
            Keyword::Unsigned => match self.next_keyword()? {
                Keyword::Int => Ok(DataType::UnsignedInt),
                Keyword::BigInt => Ok(DataType::UnsignedBig),
                bad => Err(Error::ExpectedOneOf {
                    expected: vec![
                        Token::Keyword(Keyword::Int),
                        Token::Keyword(Keyword::BigInt),
                    ],
                    found: Token::Keyword(bad),
                }),
            },
            Keyword::Bool => Ok(DataType::Boolean),
            Keyword::Varchar => {
                self.expect_token(Token::LeftParen)?;
                let length = match self.parse_expression()? {
                    Expression::Value(Value::Number(val)) => val as usize,
                    _ => Err(Error::InvalidQuery(self.position))?,
                };
                self.expect_token(Token::RightParen)?;

                Ok(DataType::VarChar(length))
            }
            _ => Err(Error::InvalidQuery(self.position)),
        }
    }

    fn parse_constrains(&mut self) -> SqlResult<Constrains> {
        match self.next_keyword()? {
            Keyword::Primary => {
                self.expect_keyword(Keyword::Key)?;
                Ok(Constrains::PrimaryKey)
            }
            Keyword::Unique => Ok(Constrains::Unique),
            bad => Err(Error::ExpectedOneOf {
                expected: vec![
                    Token::Keyword(Keyword::Primary),
                    Token::Keyword(Keyword::Unique),
                ],
                found: Token::Keyword(bad),
            }),
        }
    }

    fn parse_column(&mut self) -> SqlResult<Column> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;
        let mut constrains = vec![];

        loop {
            let constrain = self.consume_one_of(&[Keyword::Primary, Keyword::Unique]);
            match constrain {
                Keyword::Primary => {
                    self.expect_keyword(Keyword::Key)?;
                    constrains.push(Constrains::PrimaryKey);
                }
                Keyword::Unique => {
                    constrains.push(Constrains::Unique);
                }
                Keyword::None => break,
                _ => unreachable!(),
            }
        }

        Ok(Column {
            name,
            data_type,
            constrains,
        })
    }

    fn parse_where(&mut self) -> SqlResult<Option<Expression>> {
        if self.consume_optional_keyword(Keyword::Where) {
            Ok(Some(self.parse_expression()?))
        } else {
            Ok(None)
        }
    }

    fn parse_order_by(&mut self) -> SqlResult<Option<Vec<Expression>>> {
        if self.consume_optional_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            Ok(Some(self.parse_comma_separeted_values()?))
        } else {
            Ok(None)
        }
    }

    pub fn parse_statement(&mut self) -> SqlResult<Statement> {
        let statement = match self.next_keyword()? {
            Keyword::Select => {
                self.prev_token();
                self.parse_select()
            }
            Keyword::Insert => {
                self.prev_token();
                self.parse_insert()
            }
            Keyword::Update => {
                self.prev_token();
                self.parse_update()
            }
            Keyword::Delete => {
                self.prev_token();
                self.parse_delete()
            }
            Keyword::Create => {
                self.prev_token();
                self.parse_create()
            }
            Keyword::Drop => {
                self.prev_token();
                self.parse_drop()
            }
            Keyword::Begin => {
                self.expect_keyword(Keyword::Transaction)?;
                Ok(Statement::BeginTransaction)
            }
            Keyword::Rollback => Ok(Statement::Rollback),
            Keyword::Commit => Ok(Statement::Commit),
            Keyword::Explain => Ok(Statement::Explain(Box::new(self.parse_statement()?))),
            _ => todo!(),
        };

        self.expect_token(Token::SemiColon)?;

        statement
    }
}

impl<'a> StatementParser for Parser<'a> {
    fn parse_explain(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_begin(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_commit(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_create(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Create)?;

        match self.next_keyword()? {
            Keyword::Table => {
                let name = self.parse_identifier()?;

                let columns = self.parse_comma_separeted(Self::parse_column, true)?;

                let create = Create::Table { name, columns };

                Ok(Statement::Create(create))
            }
            Keyword::Unique => {
                self.expect_keyword(Keyword::Index)?;

                let name = self.parse_identifier()?;

                self.expect_keyword(Keyword::On)?;

                let table = self.parse_identifier()?;

                let column = self.parse_identifier()?;

                Ok(Statement::Create(Create::Index {
                    name,
                    table,
                    column,
                    unique: true,
                }))
            }
            Keyword::Index => {
                let name = self.parse_identifier()?;

                self.expect_keyword(Keyword::On)?;

                let table = self.parse_identifier()?;

                let column = self.parse_identifier()?;

                Ok(Statement::Create(Create::Index {
                    name,
                    table,
                    column,
                    unique: false,
                }))
            }
            Keyword::Database => {
                let name = self.parse_identifier()?;

                Ok(Statement::Create(Create::Database(name)))
            }
            keyword => Err(Error::ExpectedOneOf {
                expected: vec![
                    Token::Keyword(Keyword::Table),
                    Token::Keyword(Keyword::Table),
                ],
                found: Token::Keyword(keyword),
            })?,
        }
    }
    fn parse_drop(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Drop)?;

        match self.next_keyword()? {
            Keyword::Database => {
                let name = self.parse_identifier()?;

                Ok(Statement::Drop(Drop::Database(name)))
            }
            Keyword::Table => {
                let name = self.parse_identifier()?;

                Ok(Statement::Drop(Drop::Table(name)))
            }
            bad => Err(Error::ExpectedOneOf {
                expected: vec![
                    Token::Keyword(Keyword::Database),
                    Token::Keyword(Keyword::Table),
                ],
                found: Token::Keyword(bad),
            }),
        }
    }
    fn parse_delete(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        let from = self.parse_identifier()?;

        let r#where = self.parse_where()?;

        Ok(Statement::Delete { from, r#where })
    }
    fn parse_insert(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;

        let into = self.parse_identifier()?;

        let columns = self.parse_optional_identifier_list()?;

        self.expect_keyword(Keyword::Values)?;

        let mut values = vec![self.parse_comma_separeted(Self::parse_expression, true)?];

        while let Ok(Token::Comma) = self.peek_token() {
            self.next_token()?;
            values.push(self.parse_comma_separeted(Self::parse_expression, true)?);
        }

        Ok(Statement::Insert {
            into,
            columns,
            values,
        })
    }
    fn parse_select(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Select)?;

        let columns = self.parse_comma_separeted_values()?;

        self.expect_keyword(Keyword::From)?;

        let from = self.parse_identifier()?;

        let r#where = self.parse_where()?;

        let order_by = self.parse_order_by()?;

        Ok(Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        })
    }
    fn parse_update(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Update)?;

        let table = self.parse_identifier()?;

        self.expect_keyword(Keyword::Set)?;

        let columns = self.parse_comma_separeted(Self::parse_assignment, false)?;

        let r#where = self.parse_where()?;

        Ok(Statement::Update {
            table,
            columns,
            r#where,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_test() -> anyhow::Result<()> {
        let mut parser = Parser::new(
            "SELECT col_1, col_2, col_3 FROM test WHERE col_1 = (2 + 2 * 2) ORDER BY col_3, col_2;",
        )?;

        println!("{}", parser.parse_statement().unwrap());

        let mut parser =
            Parser::new("INSERT INTO test (col_1, col_2, col_3) VALUES (1, 2, 3), (4, 5, 6);")?;

        println!("{}", parser.parse_statement().unwrap());

        let mut parser = Parser::new("UPDATE test SET name = 'Maciek', age = 20 WHERE age >= 30;")?;

        println!("{}", parser.parse_statement().unwrap());

        let mut parser = Parser::new("DELETE FROM test WHERE age >= 30;")?;

        println!("{}", parser.parse_statement().unwrap());

        let mut parser = Parser::new(
            "CREATE TABLE test (col_1 INT PRIMARY KEY, col_2 VARCHAR(64) UNIQUE, col_3 BOOL);",
        )?;

        println!("{}", parser.parse_statement().unwrap());

        let mut parser = Parser::new("CREATE UNIQUE INDEX test_idx ON test col_1;")?;

        println!("{}", parser.parse_statement().unwrap());

        Ok(())
    }
}
