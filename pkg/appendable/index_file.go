package appendable

import (
	"io"

	"github.com/kevmo314/appendable/pkg/protocol"
	"go.uber.org/zap"
)

const CurrentVersion = 1

// IndexFile is a representation of the entire index file.
type IndexFile struct {
	Version protocol.Version

	// There is exactly one IndexHeader for each field in the data file.
	Indexes []Index

	EndByteOffsets []uint64
	Checksums      []uint64

	data io.ReadSeeker
	tail int

	Logger *zap.SugaredLogger
}

// Index is a representation of a single index.
type Index struct {
	FieldName    string
	FieldType    protocol.FieldType
	IndexRecords map[any][]protocol.IndexRecord
}

func fieldType(data any) protocol.FieldType {
	switch data.(type) {
	case string:
		return protocol.FieldTypeString
	case int, int8, int16, int32, int64, float32, float64:
		return protocol.FieldTypeNumber
	case bool:
		return protocol.FieldTypeBoolean
	case []any:
		return protocol.FieldTypeArray
	default:
		return protocol.FieldTypeObject
	}
}

func (i *IndexFile) findIndex(name string, value any) int {
	// find the index for the field
	match := -1
	for j := range i.Indexes {
		if i.Indexes[j].FieldName == name {
			match = j
			break
		}
	}
	// if the index doesn't exist, create it
	ft := fieldType(value)
	if match == -1 {
		index := Index{
			FieldName: name,
			FieldType: ft,
		}
		index.IndexRecords = make(map[any][]protocol.IndexRecord)
		i.Indexes = append(i.Indexes, index)
		return len(i.Indexes) - 1
	} else if i.Indexes[match].FieldType != ft {
		// update the field type if necessary
		i.Indexes[match].FieldType |= ft
	}
	return match

}
