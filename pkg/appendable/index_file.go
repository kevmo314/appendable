package appendable

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/kevmo314/appendable/pkg/protocol"
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

func (i *IndexFile) handleObject(dec *json.Decoder, path []string, dataIndex uint64) error {
	var dataOffset uint64
	if dataIndex > 0 {
		dataOffset = i.EndByteOffsets[dataIndex-1] + 1
	}

	// while the next token is not }, read the key
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return fmt.Errorf("failed to read token at index %d: %w", dataIndex, err)
		}

		// key must be a string
		if key, ok := key.(string); !ok {
			return fmt.Errorf("expected string key, got '%v'", key)
		} else {
			fieldOffset := dec.InputOffset() + 1 // skip the :

			value, err := dec.Token()
			if err != nil {
				return fmt.Errorf("failed to read token: %w", err)
			}

			name := strings.Join(append(path, key), ".")

			switch value := value.(type) {
			case string, int, int8, int16, int32, int64, float32, float64, bool:
				tree := i.Indexes[i.findIndex(name, value)].IndexRecords
				// append this record to the list of records for this value
				tree[value] = append(tree[value], protocol.IndexRecord{
					FieldStartByteOffset: dataOffset + uint64(fieldOffset),
					FieldLength:          int(dec.InputOffset() - fieldOffset),
				})

			case json.Token:
				switch value {
				case json.Delim('['):
					for j := range i.Indexes {
						if i.Indexes[j].FieldName == name {
							i.Indexes[j].FieldType |= protocol.FieldTypeArray
						}
					}
					// arrays are not indexed yet because we need to incorporate
					// subindexing into the specification. however, we have to
					// skip tokens until we reach the end of the array.
					depth := 1
					for {
						t, err := dec.Token()
						if err != nil {
							return fmt.Errorf("failed to read token: %w", err)
						}

						switch t {
						case json.Delim('['):
							depth++
						case json.Delim(']'):
							depth--
						}

						if depth == 0 {
							break
						}
					}
				case json.Delim('{'):
					// find the index to set the field type to unknown.
					for j := range i.Indexes {
						if i.Indexes[j].FieldName == name {
							i.Indexes[j].FieldType |= protocol.FieldTypeObject
						}
					}
					if err := i.handleObject(dec, append(path, key), dataIndex); err != nil {
						return fmt.Errorf("failed to handle object: %w", err)
					}
					// read the }
					if t, err := dec.Token(); err != nil || t != json.Delim('}') {
						return fmt.Errorf("expected '}', got '%v'", t)
					}
				default:
					return fmt.Errorf("unexpected token '%v'", value)
				}
			case nil:
				// set the field to nullable if it's not already
				for j := range i.Indexes {
					if i.Indexes[j].FieldName == name {
						i.Indexes[j].FieldType |= protocol.FieldTypeNull
					}
				}
			default:
				return fmt.Errorf("unexpected type '%T'", value)
			}
		}
	}
	return nil
}
