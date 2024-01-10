pub enum Token {
    OpenBracket,
    CloseBracket,
    Colon,
    Comma,
    String(String),
    Number(String),
    Boolean(bool),
    OpenArray,
    CloseArray,
    Null,
}

pub struct JSONTokenizer {
    input: Vec<u8>,
    position: usize,
}

impl JSONTokenizer {
    pub(crate) fn new(input: Vec<u8>) -> Self {
        Self { input, position: 0 }
    }

    pub(crate) fn next(&mut self) -> Result<Option<(Token, usize)>, String> {
        // edge case: check if we've reached the end of line
        if self.position >= self.input.len() {
            Ok(None)
        } else {
            let current_byte = self.input[self.position];

            return match current_byte {
                b'{' => {
                    self.position += 1;
                    Ok(Some((Token::OpenBracket, self.position - 1)))
                }
                b'}' => {
                    self.position += 1;
                    Ok(Some((Token::CloseBracket, self.position - 1)))
                }
                b'[' => {
                    self.position += 1;
                    Ok(Some((Token::OpenArray, self.position - 1)))
                }
                b']' => {
                    self.position += 1;
                    Ok(Some((Token::CloseArray, self.position - 1)))
                }
                b'\"' => {
                    self.position += 1;
                    self.tokenize_string()
                }
                b':' => {
                    self.position += 1;
                    Ok(Some((Token::Colon, self.position - 1)))
                }
                _ => Err(format!(
                    "Unexpected character at position {}",
                    self.position - 1
                )),
            };
        }
    }

    fn tokenize_string(&mut self) -> Result<Option<(Token, usize)>, String> {
        let start_position = self.position;

        while self.position < self.input.len() {
            let current_byte = self.input[start_position];

            match current_byte {
                b'\"' => {
                    self.position += 1;
                    Ok(Some((Token::String, start_position)))
                }
                b'\\' => {
                    self.position += 2; // skip \n
                    continue;
                }
                _ => {
                    self.position += 1;
                }
            }
        }

        Err("Unterminated string".to_string())
    }
}
