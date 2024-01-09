use crate::io::DataHandler;
use protocol::field_type::FieldFlags;
use protocol::{FieldType, IndexRecord, Version};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;

const CURRENT_VERSION: Version = 1;

pub(crate) struct Index {
    field_name: String,
    field_type: FieldFlags,
    pub(crate) index_records: HashMap<IndexKey, Vec<IndexRecord>>,
}

/// `IndexFile` is a representation of the entire index file.
pub struct IndexFile {
    version: Version,
    pub(crate) indexes: Vec<Index>,
    pub(crate) end_byte_offsets: Vec<u64>,
    pub(crate) checksums: Vec<u64>,
    data: Box<dyn DataHandler>,
    tail: u32,
}

impl IndexFile {
    pub fn new(mut data_handler: Box<dyn DataHandler>) -> Result<Self, String> {
        let mut file = IndexFile {
            version: CURRENT_VERSION,
            indexes: Vec::new(),
            data: data_handler,
            end_byte_offsets: Vec::new(),
            checksums: Vec::new(),
            tail: 0,
        };

        file.data.synchronize(
            &mut file.indexes,
            &mut file.end_byte_offsets,
            &mut file.checksums,
        )?;

        Ok(file)
    }

    pub(crate) fn find_index(&mut self, name: &str, value: &IndexKey) -> usize {
        if let Some((position, _)) = self
            .indexes
            .iter()
            .enumerate()
            .find(|(_, index)| index.field_name == name)
        {
            if !self.indexes[position]
                .field_type
                .contains(value.field_type())
            {
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
            IndexKey::Object(_) => FieldType::Object,
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
                let elements = v
                    .iter()
                    .map(|element| format!("{}", element))
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(f, "[{}]", elements)
            }
            IndexKey::Object(o) => {
                let entries = o
                    .iter()
                    .map(|(key, value)| format!("{}: {}", key, value))
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(f, "{{{}}}", entries)
            }
        }
    }
}

// todo handleJSONLObject()
// linking: https://github.com/kevmo314/appendable/blob/main/pkg/appendable/index_file.go#L77
