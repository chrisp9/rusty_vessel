use std::fmt::{Display, Formatter};
use std::iter::Scan;
use std::os::linux::raw::stat;


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
    /? $time
    /! &sym/&time/hlc3
;
    ";

    let mut tokens = Tokenizer::new(program.to_string());
    let mut lexer = Lexer::new();

    let result = tokens.tokenize();
    let lexed = lexer.lex(result);

    for r in lexed {
        println!("----");

        for v in r {
            println!("{:?}", v);
        }
    }

    println!("Hello, world!");
}

pub struct StateTransition {
    pub previous_state: LexState,
    pub current_state: LexState,
    pub expr: Option<Expr>
}

impl StateTransition {
    pub fn new(previous_state: LexState, current_state: LexState, expr: Option<Expr>) -> StateTransition{
        return StateTransition {
            previous_state,
            current_state,
            expr
        };
    }
}


pub enum LexState {
    None,
    Last {expr: Expr},
    UnaryState {kind: UnaryExprKind},
    BinaryState {kind: BinaryExprKind, lhs: Expr},
    KaryState
}

pub fn error(token: Glyph) -> Result<StateTransition, String> {
    return Err(format!("Unexpected token: {}", token))
}

impl LexState {
    pub fn transition(self, glyph: Glyph) -> Result<StateTransition, String> {
        return match self {
            LexState::None => {
                match glyph {
                    Glyph::Dollar     => Ok(self.new_unary(UnaryExprKind::Variable)),
                    Glyph::ArrayStart => error(glyph),
                    Glyph::Equals     => error(glyph),
                    Glyph::ArrayEnd   => error(glyph),
                    Glyph::Comma      => error(glyph),
                    Glyph::Slash      => error(glyph),
                    Glyph::SlashQuest => error(glyph),
                    Glyph::SlashBang  => error(glyph),
                    Glyph::Ampersand  => error(glyph),
                    Glyph::SemiColon  => error(glyph),
                    Glyph::Word(w)    => Ok(self)
                }
            },

            LexState::Last { .. } => {}
            LexState::UnaryState { .. } => {}
            LexState::BinaryState { .. } => {}
            LexState::KaryState => {}
        }
    }

    pub fn new_unary(self, kind: UnaryExprKind) -> StateTransition {
        let new_state = LexState::UnaryState {kind};
        return StateTransition::new(self, new_state, None);
    }
}


pub struct StateMachine {
    state: Option<LexState>
}

impl StateMachine {
    pub fn new() -> StateMachine {
        return StateMachine{ state: None };
    }

    pub fn transition(current: Glyph) {
        match current {

        }

    }

}

pub struct Lexer {}

impl Lexer {
    pub fn new() -> Lexer {
        return Lexer {

        };
    }

    pub fn lex(&mut self, tokens: Vec<Glyph>) -> Vec<Vec<Glyph>> {
        let statements = self.split(tokens);
        return statements;

    }

    fn split(&mut self, tokens:Vec<Glyph>) -> Vec<Vec<Glyph>> {
        let mut result = vec![];
        let mut current = vec![];

        for token in tokens {
            match token {
                Glyph::SemiColon => {
                    if current.len() > 0 {
                        result.push(current);
                        current = vec![];
                    }
                }
                t => current.push(t)
            }
        }

        return result;
    }
}


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


#[derive(Debug)]
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
    Word(String)
}

impl Display for Glyph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Dollar => write!(f, "$"),
            Equals => write!(f, "="),
            ArrayStart => write!(f, "["),
            ArrayEnd => write!(f, "]"),
            Comma => write!(f, ","),
            Slash => write!(f, "/"),
            SlashQuest => write!(f, "/?"),
            SlashBang => write!(f, "/!"),
            Ampersand => write!(f, "&"),
            SemiColon => write!(f, ";"),
            Glyph::Word(w) => write!(f, "{w}")
        }
    }
}

pub struct Tokenizer {
    idx: usize,
    statement: Vec<char>,
    word_buf: String
}

impl Tokenizer {
    pub fn new(raw: String) -> Tokenizer {
        return Tokenizer {
            idx: 0,
            statement: raw.chars().collect(),
            word_buf: "".to_string()
        };
    }

    pub fn next(&mut self) -> Option<&char> {
        let next = self.statement.get(self.idx);
        self.idx += 1;
        return next;
    }

    fn peek_at(&self, idx: usize) -> char{
        return self.statement
            .get(idx)
            .cloned().unwrap_or(' ');
    }

    pub fn tokenize(&mut self) -> Vec<Glyph> {
        let mut results = vec![];
        let mut previous_char = ' ';
        let iter = self.statement.clone().iter();

        for (index, chr) in iter.enumerate() {
            let next = self.peek_at(index + 1);

            let glyphs = match chr.clone() {
                '$' => self.next_glyph(|| Some(Glyph::Dollar)),
                '=' => self.next_glyph(|| Some(Glyph::Equals)),
                '[' => self.next_glyph(|| Some(Glyph::ArrayStart)),
                ']' => self.next_glyph(|| Some(Glyph::ArrayEnd)),
                ',' => self.next_glyph(|| Some(Glyph::Comma)),
                '&' => self.next_glyph(|| Some(Glyph::Ampersand)),
                ';' => self.next_glyph(|| Some(Glyph::SemiColon)),

                // Slashes are special - they can be used as a single
                // character '/' glyph, or a multi-character glyph
                // using '/?' or '/!'.
                '/' if next == '?' => self.next_glyph(|| Some(Glyph::SlashQuest)),
                '/' if next == '!' => self.next_glyph(|| Some(Glyph::SlashBang)),
                g @ '/' => self.next_glyph(|| Some(Glyph::Slash)),

                // Space and newline isn't lexed, but we still use these
                // to signal that a prior word has ended.
                ' '  => self.next_glyph(|| None),
                '\n' => self.next_glyph(|| None),

                // Ignore ? and ! if they follow a  / as this will be
                // handled by the / case above.
                '?'|'!' if previous_char == '/' => vec![],

                // fall-thru case - this char is part of the current word
                wildcard => self.append_to_word(wildcard.clone())
            };

            for g in glyphs {
                results.push(g)
            }

            previous_char = chr.clone();
        }

        return results;
    }

    pub fn append_to_word(&mut self, c: char) -> Vec<Glyph> {
        self.word_buf.push(c);
        return vec![];
    }

    fn next_glyph<F>(&mut self, func: F) -> Vec<Glyph> where F : FnOnce() -> Option<Glyph> {
        let mut vocabs = vec![];

        if self.word_buf.len() > 0 {
            let word = Glyph::Word(self.word_buf.clone());
            vocabs.push(word);
            self.word_buf.clear();
        }

        let next = func();
        if let Some(v) = next {
            match v {
                Glyph::Slash => (),
                v => vocabs.push(v)
            }
        }

        return vocabs;
    }
}

