package appendable

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/google/btree"
	"github.com/kevmo314/appendable/pkg/protocol"
)

const CurrentVersion = 1

// IndexFile is a representation of the entire index file.
type IndexFile struct {
	Version protocol.Version

	// There is exactly one IndexHeader for each field in the data file.
	Indexes []Index

	DataRanges []protocol.DataRange

	data io.ReadSeeker
	tail int
}

// Index is a representation of a single index.
type Index struct {
	FieldName    string
	FieldType    protocol.FieldType
	IndexRecords *btree.BTreeG[protocol.IndexRecord]
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
		return protocol.FieldTypeUnknown
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
		index.IndexRecords = btree.NewG[protocol.IndexRecord](2, index.LessFn(i.data))
		i.Indexes = append(i.Indexes, index)
		return len(i.Indexes) - 1
	} else if i.Indexes[match].FieldType != ft {
		// update the field type if necessary
		log.Printf("updating field type")
		i.Indexes[match].FieldType = protocol.FieldTypeUnknown
	}
	return match

}

func (i *IndexFile) handleObject(dec *json.Decoder, path []string, dataIndex uint64) error {
	var dataOffset uint32
	if dataIndex > 0 {
		dataOffset = uint32(i.DataRanges[dataIndex-1].EndByteOffset + 1)
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
				return fmt.Errorf("failed to read token 2: %w", err)
			}

			switch value := value.(type) {
			case string, int, int8, int16, int32, int64, float32, float64, bool:
				// find the correct spot to insert the index record
				record := protocol.IndexRecord{
					DataIndex:            dataIndex,
					FieldStartByteOffset: dataOffset + uint32(fieldOffset),
					FieldEndByteOffset:   dataOffset + uint32(dec.InputOffset()) - 1,
				}
				index := i.findIndex(strings.Join(append(path, key), "."), value)
				i.Indexes[index].IndexRecords.ReplaceOrInsert(record)

			case json.Token:
				switch value {
				case json.Delim('['):
					// arrays are not indexed yet because we need to incorporate
					// subindexing into the specification. however, we have to
					// skip tokens until we reach the end of the array.
					depth := 1
					for {
						t, err := dec.Token()
						if err != nil {
							return fmt.Errorf("failed to read token 3: %w", err)
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
					name := strings.Join(append(path, key), ".")
					for j := range i.Indexes {
						if i.Indexes[j].FieldName == name {
							i.Indexes[j].FieldType = protocol.FieldTypeUnknown
							break
						}
					}
					if err := i.handleObject(dec, append(path, key), dataIndex); err != nil {
						return err
					}
					// read the }
					if t, err := dec.Token(); err != nil || t != json.Delim('}') {
						return fmt.Errorf("expected '}', got '%v'", t)
					}
				default:
					return fmt.Errorf("unexpected token '%v'", value)
				}
			default:
				return fmt.Errorf("unexpected type '%T'", value)
			}
		}
	}
	return nil
}

func fieldRank(token json.Token) int {
	switch token.(type) {
	case nil:
		return 1
	case bool:
		return 2
	case int, int8, int16, int32, int64, float32, float64:
		return 3
	case string:
		return 4
	default:
		panic("unknown type")
	}
}

func (i *Index) LessFn(r io.ReadSeeker) btree.LessFunc[protocol.IndexRecord] {
	return func(a, b protocol.IndexRecord) bool {
		if a.DataIndex == b.DataIndex {
			// short circuit for the same data index
			return false
		}
		at, err := a.Token(r)
		if err != nil {
			panic(err)
		}
		bt, err := b.Token(r)
		if err != nil {
			panic(err)
		}
		atr := fieldRank(at)
		btr := fieldRank(bt)
		if atr != btr {
			return atr < btr
		}
		switch at.(type) {
		case nil:
			return false
		case bool:
			return !at.(bool) && bt.(bool)
		case int, int8, int16, int32, int64, float32, float64:
			return at.(float64) < bt.(float64)
		case string:
			return strings.Compare(at.(string), bt.(string)) < 0
		default:
			panic("unknown type")
		}
	}
}
