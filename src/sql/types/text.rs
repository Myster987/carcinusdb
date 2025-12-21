use std::{borrow::Borrow, ops::Deref};

#[derive(Debug, Clone, Copy)]
pub enum TextKind {
    PlainText,
    Json,
}

#[derive(Debug)]
pub struct Text {
    value: String,
    kind: TextKind,
}

impl Text {
    pub fn new(value: String) -> Self {
        Self {
            value,
            kind: TextKind::PlainText,
        }
    }

    pub fn json(value: String) -> Self {
        Self {
            value,
            kind: TextKind::Json,
        }
    }

    pub fn size(&self) -> usize {
        self.value.len()
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self {
            value: value.to_owned(),
            kind: TextKind::PlainText,
        }
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Self {
            value,
            kind: TextKind::PlainText,
        }
    }
}

impl From<Text> for String {
    fn from(value: Text) -> Self {
        value.value
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TextRef<'a> {
    value: &'a str,
    kind: TextKind,
}

impl<'a> TextRef<'a> {
    pub fn new(value: &'a str, kind: TextKind) -> Self {
        Self { value, kind }
    }

    #[inline]
    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

impl<'a> Borrow<str> for TextRef<'a> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> Deref for TextRef<'a> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

pub trait AnyText: AsRef<str> {
    fn kind(&self) -> TextKind;
}

impl AnyText for Text {
    #[inline]
    fn kind(&self) -> TextKind {
        self.kind
    }
}

impl AnyText for &str {
    #[inline]
    fn kind(&self) -> TextKind {
        TextKind::PlainText
    }
}
