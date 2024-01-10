use crate::index_file::{Index, IndexFile};
use std::io::Seek;

pub trait DataHandler: Seek {
    fn synchronize(&mut self, index_file: &mut IndexFile) -> Result<(), String>;
}
