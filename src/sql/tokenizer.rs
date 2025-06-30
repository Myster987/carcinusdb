use std::{iter::Peekable, str::Chars};

use crate::sql::token::Token;

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

    pub fn next(&mut self) -> Option<char> {
        self.chars.next().inspect(|_| self.position += 1)
    }

    pub fn peek(&mut self) -> Option<&char> {
        self.chars.peek()
    }

    pub fn position(&self) -> usize {
        self.position
    }
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut stream = Stream::new(input);
    let mut tokens = vec![];

    while let Some(ch) = stream.next() {
        match ch {
            ' ' | '\n' | '\t' => continue,
            ('0'..='9') => {
                let mut number = String::new();
                number.push(ch.to_owned());

                while let Some(digit) = stream.peek() {
                    if digit.is_numeric() {
                        number.push(stream.next().unwrap());
                    } else {
                        break;
                    }
                }

                tokens.push(Token::Number(number));
            }
            '=' => {
                if let Some(nch) = stream.peek() {
                    if *nch == '=' {
                        let _ = stream.next();
                    }
                }
                tokens.push(Token::Eq);
            },
            '!' => {
                if let Some(nch) = stream.peek() {
                    if *nch == '=' {
                        let _ = stream.next();
                        tokens.push(Token::Neq);
                    }
                }
            },
            '>' => {
                if let Some(nch) = stream.peek() {
                    if *nch == '=' {
                        let _ = stream.next();
                        tokens.push(Token::GtEq);
                    } else {
                        tokens.push(Token::Gt);
                    }
                }
            }
            '<' => {
                if let Some(nch) = stream.peek() {
                    if *nch == '=' {
                        let _ = stream.next();
                        tokens.push(Token::LtEq);
                    } else {
                        tokens.push(Token::Lt);
                    }
                }
            }
            '+' => tokens.push(Token::Add),
            '-' => tokens.push(Token::Sub),
            '*' => tokens.push(Token::Mul),
            '/' => tokens.push(Token::Div),
            '(' => tokens.push(Token::LeftParen),
            ')' => tokens.push(Token::RightParen),
            ',' => tokens.push(Token::Comma),
            ';' => tokens.push(Token::SemiColon),
            _ => {}
        }
    }

    tokens.push(Token::Eof);

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() -> anyhow::Result<()> {
        let stream = "2 + 123 * 2 >= 5 / 2;";

        let tokens = tokenize(stream);

        println!("{:?}", tokens);

        Ok(())
    }
}
