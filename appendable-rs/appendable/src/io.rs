use crate::index_file::Index;
use std::io::{Read, Seek};

pub trait DataHandler: Seek {
    fn synchronize(
        &mut self,
        indexes: &mut Vec<Index>,
        end_byte_offsets: &mut Vec<u64>,
        checksums: &mut Vec<u64>,
    ) -> Result<(), String>;
}
