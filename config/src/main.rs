pub mod lex;
pub mod domain;

use std::fmt::{Display, Formatter};
use std::fs::read;
use std::iter::Scan;
use std::os::linux::raw::stat;
use crate::domain::Glyph;
use crate::lex::input_stream::InputStream;
use crate::lex::lexer::Lexer;
use crate::lex::Tokenizer;


fn main() {
    let program = "

$sym = [btc, eth, ltc];
$time = [1min, 5min, 15min];

$sym
    /? [open, high, low, close, volume]
    /! $time
;

$sym
    /? [high, low, close]
    /? it $time &time
    /! &sym/&time/hlc3
;
    ";

    let stream = InputStream::new(program.to_string().chars().collect());
    let reader = Tokenizer::new(stream);

    let glyphs = reader.read_all();

    for item in glyphs {
        println!("{item}");
    }

    println!("Hello, world!");
}

pub enum OperatorKind {
    Slash,
    SlashQuest,
    SlashBang
}

pub enum Word {
    None,
    Var { name: String },
    Operator {op: String, kind: OperatorKind},
    Literal {value: Literal},
    Array {values: Vec<Literal>}
}

pub struct Literal {
    value: String
}

impl Literal {
    pub fn new(value: String) -> Literal {
        return Literal {value};
    }
}


