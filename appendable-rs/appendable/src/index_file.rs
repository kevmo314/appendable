use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use protocol::{Version, FieldType, IndexRecord};
use protocol::field_type::FieldFlags;
use crate::io::ReadSeek;

const CURRENT_VERSION: u64 = 1;

/// `IndexFile` is a representation of the entire index file.
pub struct IndexFile {
    version: Version,
    indexes: Vec<Index>,
    end_byte_offsets: Vec<u64>,
    checksums: Vec<u64>,
    data: Box<dyn ReadSeek>,
    tail: u32,
}

impl IndexFile {
    fn find_index(&mut self, name: &str, value: &IndexKey) -> usize {
        if let Some((position, _)) = self.indexes
            .iter()
            .enumerate()
            .find(|(_, index)| index.field_name == name) {

            if !self.indexes[position].field_type.contains(value.field_type()) {
                self.indexes[position].field_type.set(value.field_type());
            }

            position
        } else {
            let mut new_index = Index {
                field_name: name.to_string(),
                field_type: FieldFlags::new(),
                index_records: HashMap::new(),
            };

            new_index.field_type.set(value.field_type());
            self.indexes.push(new_index);
            self.indexes.len() - 1
        }
    }
}

/// `IndexKey` addresses the dynamic typing of keys in `IndexRecord` by stating all possible variants
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum IndexKey {
    String(String),
    Number(String),
    Boolean(bool),
    Array(Vec<IndexKey>),
    Object(HashMap<String, IndexKey>),
}

impl IndexKey {
    fn field_type(&self) -> FieldType {
        match self {
            IndexKey::String(_) => FieldType::String,
            IndexKey::Number(_) => FieldType::Number,
            IndexKey::Boolean(_) => FieldType::Boolean,
            IndexKey::Array(_) => FieldType::Array,
            IndexKey::Object(_) => FieldType::Object
        }
    }
}

impl fmt::Display for IndexKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            IndexKey::String(s) => write!(f, "{}", s),
            IndexKey::Number(n) => write!(f, "{}", n),
            IndexKey::Boolean(b) => write!(f, "{}", b),
            IndexKey::Array(v) => {
                let elements = v.iter()
                    .map(|element| format!("{}", element))
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(f, "[{}]", elements)
            },
            IndexKey::Object(o) => {
                let entries = o.iter()
                    .map(|(key, value)| format!("{}: {}", key, value))
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(f, "{{{}}}", entries)
            }
        }
    }
}

struct Index {
    field_name: String,
    field_type: FieldFlags,
    index_records: HashMap<IndexKey, Vec<IndexRecord>>
}



// todo handleJSONLObject()
// linking: https://github.com/kevmo314/appendable/blob/main/pkg/appendable/index_file.go#L77