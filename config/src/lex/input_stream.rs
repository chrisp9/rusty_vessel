pub struct InputStream {
    input: Vec<char>
}

impl InputStream {
    pub fn new(input: String) -> InputStream {
        return InputStream {
            input: input.chars().collect(),
        }
    }

    pub fn next(&self, cursor: &InputStreamCursor) -> InputStreamCursor {
        let mut curPos = cursor.pos;
        let mut line = cursor.line;
        let mut col = cursor.col;

        curPos +=1;

        let chr = self.input[curPos];

        if chr == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }

        return InputStreamCursor::new(
            curPos,
            line,
            col,
            chr
        );
    }

    pub fn peek(&self, cursor: &InputStreamCursor) -> char{
        return self.input[cursor.pos];
    }

    pub fn eof(&self, cursor: &InputStreamCursor) -> bool {
        return cursor.pos >= self.input.len() - 1;
    }

    pub fn croak(&self, cursor: InputStreamCursor, msg: String) {
        let line = cursor.line;
        let col = cursor.col;

        panic!("{msg} ({line}:{col})");
    }
}

pub struct InputStreamCursor {
    pub pos: usize,
    pub line: usize,
    pub col: usize,
    pub val: char
}

impl InputStreamCursor {
    pub fn new(pos: usize, line: usize, col: usize, val: char) -> InputStreamCursor {
        return InputStreamCursor {
            pos,
            line,
            col,
            val
        }
    }

    pub fn start() -> InputStreamCursor {
        return InputStreamCursor {
            pos: 0,
            line: 1,
            col: 1,
            val: ' '
        };
    }
}
