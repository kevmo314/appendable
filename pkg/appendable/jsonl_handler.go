package appendable

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/kevmo314/appendable/pkg/protocol"
)

type JSONLHandler struct {
	io.ReadSeeker
}

func (j JSONLHandler) Synchronize(f *IndexFile) error {

	// read until the next newline
	scanner := bufio.NewScanner(f.data)
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Bytes()

		// create a new json decoder
		dec := json.NewDecoder(bytes.NewReader(line))

		existingCount := len(f.EndByteOffsets)

		// append a data range
		var start uint64
		if len(f.EndByteOffsets) > 0 {
			start = f.EndByteOffsets[existingCount-1]
		}
		f.EndByteOffsets = append(f.EndByteOffsets, start+uint64(len(line))+1)
		f.Checksums = append(f.Checksums, xxhash.Sum64(line))

		// if the first token is not {, then return an error
		if t, err := dec.Token(); err != nil || t != json.Delim('{') {
			return fmt.Errorf("expected '%U', got '%U' (only json objects are supported at the root)", '{', t)
		}

		if err := f.handleJSONLObject(dec, []string{}, uint64(existingCount), start); err != nil {
			return fmt.Errorf("failed to handle object: %w", err)
		}

		// the next token must be a }
		if t, err := dec.Token(); err != nil || t != json.Delim('}') {
			return fmt.Errorf("expected '}', got '%v'", t)
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

func (i *IndexFile) handleJSONLObject(dec *json.Decoder, path []string, dataIndex, dataOffset uint64) error {
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
					DataNumber:           dataIndex,
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
					if err := i.handleJSONLObject(dec, append(path, key), dataIndex, dataOffset); err != nil {
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
