use crate::index_file::IndexFile;
use crate::io::DataHandler;
use std::io::{BufRead, BufReader, Cursor, Seek, SeekFrom};
use xxhash_rust::xxh3::Xxh3;

pub struct JSONLHandler {
    // todo! change to borrowed type like &[u8] -- spent too long battling lifetimes
    reader: BufReader<Cursor<Vec<u8>>>,
    xxh3: Xxh3,
}
impl JSONLHandler {
    pub fn new(data: Vec<u8>) -> Self {
        JSONLHandler {
            reader: BufReader::new(Cursor::new(data)),
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
    fn synchronize(&mut self, index_file: &mut IndexFile) -> Result<(), String> {
        let mut line = String::new();
        let mut start_offset: u64 = 0;

        while self
            .reader
            .read_line(&mut line)
            .map_err(|e| e.to_string())?
            > 0
        {
            let existing_count = index_file.end_byte_offsets.len();
            // compute byte_offset for current line
            let line_length = line.as_bytes().len() as u64;
            let current_offset = start_offset + line_length + 1;
            index_file.end_byte_offsets.push(current_offset);

            // compute checksum
            self.xxh3.update(line.as_bytes());
            let checksum = self.xxh3.digest(); // produce the final hash value
            index_file.checksums.push(checksum);

            // Process the JSON line and update indexes
            handle_json_object(
                line.into_bytes(),
                index_file,
                &mut vec![],
                existing_count as u64,
                start_offset,
            )?;

            start_offset = current_offset;
            line.clear();
        }

        Ok(())
    }
}

fn handle_json_object(
    json_line: Vec<u8>,
    index_file: &mut IndexFile,
    path: &mut Vec<String>,
    data_index: u64,
    data_offset: u64,
) -> Result<usize, String> {
    Ok(1)
}
