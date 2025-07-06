use crate::sql::{
    Error, SqlResult,
    statement::{BinaryOperator, Expression, Statement, UnaryOperator, Value},
    token::{Keyword, Token},
    tokenizer::Tokenizer,
};

pub trait StatementParser {
    fn parse_explain(&mut self) -> SqlResult<Statement>;
    fn parse_while(&mut self) -> SqlResult<Statement>;
    fn parse_select(&mut self) -> SqlResult<Statement>;
    fn parse_create(&mut self) -> SqlResult<Statement>;
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
    pub fn validate(token_stream: Vec<SqlResult<Token>>) -> SqlResult<Vec<Token>> {
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

    pub fn parse_statement(&mut self) -> SqlResult<Statement> {
        let statement = match self.next_token()? {
            Token::Keyword(k) => match k {
                Keyword::Select => {
                    self.prev_token();
                    self.parse_select()
                }
                _ => todo!(),
            },
            _ => todo!(),
        };

        self.expect_token(Token::SemiColon)?;

        statement
    }

    fn parse_identifier(&mut self) -> SqlResult<String> {
        self.next_token().and_then(|token| match token {
            Token::Identifier(value) => Ok(value),
            _ => Err(Error::InvalidQuery(self.position)),
        })
    }

    fn parese_comma_separeted_values(&mut self, required_parenthesis: bool) -> SqlResult<Vec<Expression>> {
        if required_parenthesis {
            self.expect_token(Token::LeftParen)?;
        }
        
        let mut result = vec![self.parse_expresion()?];

        while let Ok(Token::Comma) = self.peek_token() {
            self.next_token()?;
            result.push(self.parse_expresion()?);
        }
        
        if required_parenthesis {
            self.expect_token(Token::RightParen)?;
        }

        Ok(result)
    }

    /// Initialized TDOP descent. This function starts recursively calling other functions.
    fn parse_expresion(&mut self) -> SqlResult<Expression> {
        self.parse_expr(0)
    }

    fn parse_expr(&mut self, precedence: u8) -> SqlResult<Expression> {
        let mut expr = self.parse_prefix()?;
        let mut next_precedence = self.get_next_precedence();

        while precedence < next_precedence {
            expr = self.parese_infix(expr, next_precedence)?;
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
                let expression = self.parse_expresion()?;
                self.expect_token(Token::RightParen)?;
                Ok(Expression::Nested(Box::new(expression)))
            }

            _ => Err(Error::InvalidQuery(self.position)),
        }
    }

    fn parese_infix(&mut self, left: Expression, precedence: u8) -> SqlResult<Expression> {
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
        todo!()
    }
    fn parse_delete(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_insert(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_select(&mut self) -> SqlResult<Statement> {
        self.expect_keyword(Keyword::Select)?;

        let columns = self.parese_comma_separeted_values(false)?;

        self.expect_keyword(Keyword::From)?;

        let from = self.parse_identifier()?;

        Ok(Statement::Select { columns, from, r#where: None, order_by: None })
    }
    fn parse_update(&mut self) -> SqlResult<Statement> {
        todo!()
    }
    fn parse_while(&mut self) -> SqlResult<Statement> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_test() -> anyhow::Result<()> {
        let mut parser = Parser::new("SELECT col_1, col_2, col_3 FROM test;")?;

        println!("{:?}", parser.parse_statement());

        Ok(())
    }
}
