use crate::Glyph;
use crate::lex::input_stream::InputStream;

pub mod tokenizer;
pub mod input_stream;

pub struct Tokenizer {
    pub stream: InputStream
}

impl Tokenizer {
    pub fn new(stream: InputStream) -> Tokenizer {
        return Tokenizer {stream};
    }

    pub fn read_all(&self) -> Vec<Glyph>{
        let mut cursor = InputStreamCursor::start();
        let mut word: Option<String> = None;
        let mut glyphs = vec![];

        while !self.stream.eof(&cursor) {
            let chr = cursor.val;
            cursor = self.stream.next(&cursor);

            let current_char = match chr {
                '$' => Some(Glyph::Dollar),
                '=' => Some(Glyph::Equals),
                '[' => Some(Glyph::ArrayStart),
                ']' => Some(Glyph::ArrayEnd),
                ',' => Some(Glyph::Comma),
                '&' => Some(Glyph::Ampersand),
                ';' => Some(Glyph::SemiColon),

                // Slashes are special - they can be used as a single
                // character '/' glyph, or a multi-character glyph
                // using '/?' or '/!'.
                '/' if cursor.val == '?' => Some(Glyph::SlashQuest),
                '/' if cursor.val == '!' => Some(Glyph::SlashBang),
                '/' => Some(Glyph::Slash),

                // Space and newline isn't lexed, but we still use these
                // to signal that a prior word has ended.
                ' '  => None,
                '\n' => None,

                // Ignore ? and ! if they follow a  / as this will be
                // handled by the / case above. Panic because reaching this
                // indicates a logic bug
                '?'|'!' => None,

                wildcard => {
                    let ret = match &word {
                        None => Some(wildcard.to_string()),
                        Some(v) => Some(format!("{v}{wildcard}"))
                    }.clone();

                    word = ret;
                    None
                },
            };

            // If the current Glyph is not a Word, we may have reached the
            // next
            if let Some(v) = current_char {
                match v {
                    Glyph::Word(v) => {},
                    _ => {
                        if let Some(v) = &word {
                            glyphs.push(Glyph::Word(v.clone()));
                            word = None;
                        }

                        glyphs.push(v);
                    }
                }
            }
        }

        return glyphs;
    }
}

