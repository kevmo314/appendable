use crate::field_type::FieldType;

/*
The overall index file for AppendableDB is structured as:

+-----------------------+
| Version               |
+-----------------------+
| IndexFileHeader       |
+-----------------------+
| IndexHeader           |
+-----------------------+
|        ...            |
+-----------------------+
| IndexHeader           |
+-----------------------+
| IndexRecord           |
+-----------------------+
|        ...            |
+-----------------------+
| IndexRecord           |
+-----------------------+
| EndByteOffset         |
+-----------------------+
|        ...            |
+-----------------------+
| EndByteOffset         |
+-----------------------+
| Checksum              |
+-----------------------+
|        ...            |
+-----------------------+
| Checksum              |
+-----------------------+
*/

/// `Version` is the version of AppendableDB this library is compatible with.
pub type Version = u8;

/// `IndexFileHeader` is the header of the index file.
///
/// # Attributes
/// - `index_length` represents the number of bytes the `IndexHeader` occupy
/// - `data_count` represents the number of data records indexed by this index file
pub struct IndexFileHeader {
    index_length: u64,
    data_count: u64,
}

/// `IndexHeader` is the header of each index record. This represents the field available in the data file.
///
/// # Attributes
/// - `field_type` represents the type of data stored in the field. Note that the field data doesn't need to follow this type, but it is used to determine the Typescript typings for the field.
pub struct IndexHeader {
    field_name: String,
    field_type: FieldType,
    index_record_count: u64,
}

/// `IndexRecord`
///
/// # Attributes
/// - `field_start_byte_offset` represents the byte offset of the field in the data file to fetch exactly in the field value.
/// - `field_length` is pessimistic: it is encoded value that is at least as long as the actual field value.
pub struct IndexRecord {
    data_number: u64,
    field_start_byte_offset: u64,
    field_length: u64,
}

// Todo! write out JSON Token() implementation
// Linking: https://github.com/kevmo314/appendable/blob/main/pkg/protocol/protocol.go#L123
