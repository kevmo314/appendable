package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/google/btree"
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
| DataRange             |
+-----------------------+
|        ...            |
+-----------------------+
| DataRange             |
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
type FieldType byte

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeString
	FieldTypeNumber
	FieldTypeArray
	FieldTypeBoolean
)

func (t FieldType) LessFn(r io.ReadSeeker) btree.LessFunc[IndexRecord] {
	return func(a, b IndexRecord) bool {
		switch t {
		case FieldTypeString:
			var avalue, bvalue string
			if err := a.Decode(r, &avalue); err != nil {
				panic(err)
			}
			if err := b.Decode(r, &bvalue); err != nil {
				panic(err)
			}
			return strings.Compare(avalue, bvalue) < 0
		case FieldTypeNumber:
			var avalue, bvalue float64
			if err := a.Decode(r, &avalue); err != nil {
				panic(err)
			}
			if err := b.Decode(r, &bvalue); err != nil {
				panic(err)
			}
			return avalue < bvalue
		case FieldTypeArray:
			panic("not implemented")
		case FieldTypeBoolean:
			var avalue, bvalue bool
			if err := a.Decode(r, &avalue); err != nil {
				panic(err)
			}
			if err := b.Decode(r, &bvalue); err != nil {
				panic(err)
			}
			return !avalue && bvalue
		default:
			panic("unknown field type")
		}
	}
}

type IndexRecord struct {
	// DataRange represents the range of bytes in the data file that this index
	// record points to.
	DataIndex uint64
	// FieldByteOffset represents the byte offset of the field in the data
	// record to fetch exactly the field value. That is, the field value is
	// stored at DataRange.StartByteOffset + FieldByteOffset.
	FieldStartByteOffset uint32
	FieldEndByteOffset   uint32
}

func (i IndexRecord) Decode(r io.ReadSeeker, v any) error {
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current offset: %w", err)
	}
	if _, err := r.Seek(int64(i.FieldStartByteOffset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to field start byte offset: %w", err)
	}
	if err := json.NewDecoder(io.LimitReader(r, int64(i.FieldEndByteOffset-i.FieldStartByteOffset+1))).Decode(v); err != nil {
		return fmt.Errorf("failed to decode field: %w", err)
	}
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to original offset: %w", err)
	}
	return nil
}

type DataRange struct {
	// EndByteOffset represents the end byte offset of the data record in the
	// data file. We choose to not use the length here to make it easier to
	// randomly access data records.
	EndByteOffset uint64
	Checksum      uint64
}
