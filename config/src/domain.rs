use std::fmt::{Debug, Display, Formatter};

pub enum UnaryExprKind {
    Variable
}

pub enum BinaryExprKind {
    Equals
}

pub enum Expr {
    UnaryExpr(UnaryExprKind, Box<Expr>),
    BinaryExpr(BinaryExprKind, Box<Expr>, Box<Expr>),
    KaryExpr(Vec<Expr>),
    Literal(String)
}


#[derive(Clone)]
pub enum Glyph {
    Dollar,
    Equals,
    ArrayStart,
    ArrayEnd,
    Comma,
    Slash,
    SlashQuest,
    SlashBang,
    Ampersand,
    SemiColon,
    Word(String),
}

impl Display for Glyph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Glyph::Dollar => write!(f, "$"),
            Glyph::Equals => write!(f, "="),
            Glyph::ArrayStart => write!(f, "["),
            Glyph::ArrayEnd => write!(f, "]"),
            Glyph::Comma => write!(f, ","),
            Glyph::Slash => write!(f, "/"),
            Glyph::SlashQuest => write!(f, "/?"),
            Glyph::SlashBang => write!(f, "/!"),
            Glyph::Ampersand => write!(f, "&"),
            Glyph::SemiColon => write!(f, ";"),
            Glyph::Word(w) => write!(f, "{w}")
        }
    }
}

impl Debug for Glyph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            Glyph::Dollar => write!(f, "$"),
            Glyph::Equals => write!(f, "="),
            Glyph::ArrayStart => write!(f, "["),
            Glyph::ArrayEnd => write!(f, "]"),
            Glyph::Comma => write!(f, ","),
            Glyph::Slash => write!(f, "/"),
            Glyph::SlashQuest => write!(f, "/?"),
            Glyph::SlashBang => write!(f, "/!"),
            Glyph::Ampersand => write!(f, "&"),
            Glyph::SemiColon => write!(f, ";"),
            Glyph::Word(w) => write!(f, "{w}")
        }
    }
}