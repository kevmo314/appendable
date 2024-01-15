package protocol

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

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

// Version is the version of AppendableDB this library is compatible with.
type Version byte

// IndexFileHeader is the header of the index file.
type IndexFileHeader struct {
	// IndexLength represents the number of bytes the IndexHeaders occupy.
	IndexLength uint64

	// DataCount represents the number of data records indexed by this index
	// file.
	DataCount uint64
}

// IndexHeader is the header of each index record. This represents the fields
// available in the data file.
type IndexHeader struct {
	FieldName string

	// FieldType represents the type of data stored in the field. Note that the
	// field data doesn't need to follow this type, but it is used to determine
	// the TypeScript typings for the field.
	FieldType FieldType

	IndexRecordCount uint64
}

// FieldType represents the type of data stored in the field, which follows
// JSON types excluding Object and null. Object is broken down into subfields
// and null is not stored.
//
// FieldType is left as a uint64 to avoid shooting ourselves in the foot if we
// want to support more types in the future via other file formats.
type FieldType uint64

const (
	FieldTypeString FieldType = (1 << iota)
	FieldTypeNumber
	FieldTypeObject
	FieldTypeArray
	FieldTypeBoolean
	FieldTypeNull
)

func (t FieldType) TypescriptType() string {
	components := []string{}
	if t&FieldTypeString != 0 {
		components = append(components, "string")
	}
	if t&FieldTypeNumber != 0 {
		components = append(components, "number")
	}
	if t&FieldTypeObject != 0 {
		components = append(components, "Record")
	}
	if t&FieldTypeArray != 0 {
		components = append(components, "any[]")
	}
	if t&FieldTypeBoolean != 0 {
		components = append(components, "boolean")
	}
	if t&FieldTypeNull != 0 {
		components = append(components, "null")
	}
	if len(components) == 0 {
		return "unknown"
	}
	return strings.Join(components, " | ")
}

type IndexRecord struct {
	DataNumber uint64
	// FieldByteOffset represents the byte offset of the field in the data
	// file to fetch exactly the field value.
	FieldStartByteOffset uint64
	// FieldLength is pessimistic: it is an encoded value that is at least as
	// long as the actual field value.
	FieldLength int
}

func (i IndexRecord) CSVField(r io.ReadSeeker) (any, error) {
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current offset: %w", err)
	}

	if _, err := r.Seek(int64(i.FieldStartByteOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to field start byte offset: %w", err)
	}

	fields, err := csv.NewReader(io.LimitReader(r, int64(i.FieldLength))).Read()
	if err != nil {
		return nil, fmt.Errorf("failed to decode field: %w", err)
	}

	fmt.Printf("Fields read at offset %d: %v\n", i.FieldStartByteOffset, fields)

	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to original offset: %w", err)
	}

	return fields[0], nil
}

func (i IndexRecord) Token(r io.ReadSeeker) (json.Token, error) {
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current offset: %w", err)
	}
	if _, err := r.Seek(int64(i.FieldStartByteOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to field start byte offset: %w", err)
	}
	token, err := json.NewDecoder(io.LimitReader(r, int64(i.FieldLength))).Token()
	if err != nil {
		return nil, fmt.Errorf("failed to decode field: %w", err)
	}
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to original offset: %w", err)
	}
	return token, nil
}
