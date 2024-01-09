use crate::index_file::{Index, IndexFile, IndexKey};
use crate::io::DataHandler;
use serde_json::{Deserializer, Map, Value};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use twox_hash::xxh3::hash64;

pub struct JSONLHandler {
    pub file: File,
    pub buffer: Option<Vec<u8>>,
}
impl JSONLHandler {
    pub fn new(file: File) -> Self {
        JSONLHandler { file, buffer: None }
    }

    /// we need to read the entire file to buffer to compute byte slices for checksums
    pub fn read_file_to_buffer(&mut self) -> Result<(), String> {
        let mut buffer = Vec::new();
        self.file
            .read_to_end(&mut buffer)
            .map_err(|e| e.to_string())?;

        self.buffer = Some(buffer);

        Ok(())
    }
}
impl Seek for JSONLHandler {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}
impl DataHandler for JSONLHandler {
    fn synchronize(
        &mut self,
        indexes: &mut Vec<Index>,
        end_byte_offsets: &mut Vec<u64>,
        checksums: &mut Vec<u64>,
    ) -> Result<(), String> {
        if self.buffer.is_none() {
            self.read_file_to_buffer()?;
        }

        let buffer = self.buffer.as_ref().unwrap();

        let mut deserializer = Deserializer::from_slice(buffer).into_iter::<Value>();

        let mut start_offset = 0;

        while let Some(result) = deserializer.next() {
            let value = result.map_err(|e| e.to_string())?;
            let end_offset = deserializer.byte_offset();

            let existing_count = end_byte_offsets.len();

            // since the `StreamDeserializer` parses JSON values rather than lines, we don't have direct access to the original line strings
            // this is a workaround where we find the byte slice from the start and end byte offset
            let slice = &buffer[start_offset..end_offset];

            let checksum = hash64(slice);
            checksums.push(checksum);

            end_byte_offsets.push(end_offset as u64);


            if let Value::Object(obj) = value {
                handle_json_object(
                    &obj,
                    indexes,
                    vec![],
                    existing_count as u64,
                    start_offset as u64,
                )?;
            } else {
                return Err("expected a JSON object".to_string());
            }

            start_offset = end_offset;
        };

        Ok(())
    }
}

fn handle_json_object(
    obj: &Map<String, Value>,
    indexes: &mut Vec<Index>,
    path: Vec<String>,
    data_index: u64,
    data_offset: u64,
) -> Result<(), String> {
    for (key, value) in obj {
        let field_offset = data_offset; // todo ask kevin about best way of incrementing this
        let name = path
            .iter()
            .chain(std::iter::once(key))
            .cloned()
            .collect::<Vec<_>>()
            .join(".");

        match value {
            Value::String(s) => {
                // Handle string value
            }
            Value::Number(n) => {
                // Handle number value
            }
            Value::Bool(b) => {
                // Handle boolean value
            }
            Value::Array(arr) => {
                // Handle array - might involve recursion
            }
            Value::Object(nested_obj) => {
                // Recursively handle nested object
                handle_json_object(nested_obj, indexes, vec![name], data_index, field_offset)?;
            }
            Value::Null => {
                // Handle null value
            }
            _ => return Err(format!("Unexpected type: {}", value)),
        }
    }

    Ok(())
}
