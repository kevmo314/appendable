package appendable

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cespare/xxhash/v2"
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

	less btree.LessFunc[protocol.IndexRecord]
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
	if match == -1 {
		i.Indexes = append(i.Indexes, Index{
			FieldName:    name,
			FieldType:    fieldType(value),
			IndexRecords: btree.NewG[protocol.IndexRecord](2, i.less),
		})
		return len(i.Indexes) - 1
	} else if i.Indexes[match].FieldType != fieldType(value) {
		// update the field type if necessary
		i.Indexes[match].FieldType = protocol.FieldTypeUnknown
	}
	return match

}

func (i *IndexFile) handleObject(dec *json.Decoder, path []string, dataIndex uint64) error {
	// while the next token is not }, read the key
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %w", err)
		}

		// key must be a string
		if key, ok := key.(string); !ok {
			return fmt.Errorf("expected string key, got '%v'", key)
		} else {
			fieldOffset := dec.InputOffset()

			value, err := dec.Token()
			if err != nil {
				return fmt.Errorf("failed to read token: %w", err)
			}

			switch value := value.(type) {
			case string, int, int8, int16, int32, int64, float32, float64, bool:
				// find the correct spot to insert the index record
				i.Indexes[i.findIndex(strings.Join(append(path, key), "."), value)].IndexRecords.ReplaceOrInsert(protocol.IndexRecord{
					DataIndex:            dataIndex,
					FieldStartByteOffset: uint32(fieldOffset),
					FieldEndByteOffset:   uint32(dec.InputOffset()) - 1,
				})

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

func (i *IndexFile) AppendDataRow(r io.Reader) (map[string]any, error) {
	w := &bytes.Buffer{}

	// create a new json decoder
	dec := json.NewDecoder(io.TeeReader(r, w))

	// if the first token is not {, then return an error
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, fmt.Errorf("expected '%U', got '%U' (only json objects are supported at the root)", '{', t)
	}

	if err := i.handleObject(dec, []string{}, uint64(len(i.DataRanges))); err != nil {
		return nil, err
	}

	// the next token must be a }
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return nil, fmt.Errorf("expected '}', got '%v'", t)
	}

	// append a data range
	var start uint64
	if len(i.DataRanges) > 0 {
		start = i.DataRanges[len(i.DataRanges)-1].EndByteOffset + 1
	}
	i.DataRanges = append(i.DataRanges, protocol.DataRange{
		StartByteOffset: start,
		EndByteOffset:   start + uint64(w.Len()) - 1,
		Checksum:        xxhash.Sum64(w.Bytes()),
	})

	return nil, nil
}

func (i *IndexFile) Validate(r io.Reader) int {
	s := bufio.NewScanner(r)
	for j := 0; j < len(i.DataRanges) && s.Scan(); j++ {
		if xxhash.Sum64(s.Bytes()) != i.DataRanges[j].Checksum {
			return j
		}
	}
	return len(i.DataRanges)
}
