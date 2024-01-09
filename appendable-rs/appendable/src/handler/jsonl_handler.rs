use crate::index_file::Index;
use crate::io::DataHandler;
use serde_json::{Deserializer, Map, Value};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use xxhash_rust::xxh3::Xxh3;

pub struct JSONLHandler {
    reader: BufReader<File>,
    xxh3: Xxh3,
}
impl JSONLHandler {
    pub fn new(file: File) -> Self {
        JSONLHandler {
            reader: BufReader::new(file),
            xxh3: Xxh3::new(),
        }
    }
}
impl Seek for JSONLHandler {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(pos)
    }
}
impl DataHandler for JSONLHandler {
    fn synchronize(
        &mut self,
        indexes: &mut Vec<Index>,
        end_byte_offsets: &mut Vec<u64>,
        checksums: &mut Vec<u64>,
    ) -> Result<(), String> {
        let mut line = String::new();
        let mut start_offset: u64 = 0;

        while self
            .reader
            .read_line(&mut line)
            .map_err(|e| e.to_string())?
            > 0
        {
            let existing_count = end_byte_offsets.len();
            // compute byte_offset for current line
            let line_length = line.as_bytes().len() as u64;
            let current_offset = start_offset + line_length + 1;
            end_byte_offsets.push(current_offset);

            // compute checksum
            self.xxh3.update(line.as_bytes());
            let checksum = self.xxh3.digest(); // produce the final hash value
            checksums.push(checksum);

            // Process the JSON line and update indexes
            handle_json_object(&line, indexes, vec![], existing_count as u64, start_offset)?;

            start_offset = current_offset;
            line.clear();
        }

        Ok(())
    }
}

fn handle_json_object(
    json_line: &str,
    indexes: &mut Vec<Index>,
    path: Vec<String>,
    data_index: u64,
    data_offset: u64,
) -> Result<(), String> {
    Ok(())
}
