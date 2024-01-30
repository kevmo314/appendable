package handlers

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
)

type JSONLHandler struct {
}

var _ appendable.DataHandler = (*JSONLHandler)(nil)

func (j JSONLHandler) Format() appendable.Format {
	return appendable.FormatJSONL
}

func (j JSONLHandler) Synchronize(f *appendable.IndexFile, df appendable.DataFile) error {
	// read until the next newline
	metadata, err := f.Metadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	if _, err := df.Seek(int64(metadata.ReadOffset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}
	scanner := bufio.NewScanner(df)
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Bytes()

		// create a new json decoder
		dec := json.NewDecoder(bytes.NewReader(line))

		// if the first token is not {, then return an error
		if t, err := dec.Token(); err != nil || t != json.Delim('{') {
			return fmt.Errorf("expected '%U', got '%U' (only json objects are supported at the root)", '{', t)
		}

		if err := handleJSONLObject(f, df, dec, []string{}, btree.MemoryPointer{
			Offset: metadata.ReadOffset,
			Length: uint32(len(line)),
		}); err != nil {
			return fmt.Errorf("failed to handle object: %w", err)
		}

		// the next token must be a }
		if t, err := dec.Token(); err != nil || t != json.Delim('}') {
			return fmt.Errorf("expected '}', got '%v'", t)
		}

		metadata.ReadOffset += uint64(len(line)) + 1 // include the newline

		slog.Info("read line", "i", i, "offset", metadata.ReadOffset)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	// update the metadata
	if err := f.SetMetadata(metadata); err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	return nil
}

func jsonTypeToFieldType(t json.Token) appendable.FieldType {
	switch t.(type) {
	case json.Delim:
		switch t {
		case json.Delim('{'):
			return appendable.FieldTypeObject
		case json.Delim('['):
			return appendable.FieldTypeArray
		}
	case json.Number, float64:
		return appendable.FieldTypeFloat64
	case string:
		return appendable.FieldTypeString
	case bool:
		return appendable.FieldTypeBoolean
	case nil:
		return appendable.FieldTypeNull
	}
	panic(fmt.Sprintf("unexpected token '%v'", t))
}

func handleJSONLObject(f *appendable.IndexFile, r io.ReaderAt, dec *json.Decoder, path []string, data btree.MemoryPointer) error {
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
			fieldOffset := dec.InputOffset() + 1 // skip the :

			value, err := dec.Token()
			if err != nil {
				return fmt.Errorf("failed to read token: %w", err)
			}

			name := strings.Join(append(path, key), ".")

			if name != "VendorID" {
				continue
			}

			page, err := f.FindOrCreateIndex(name, jsonTypeToFieldType(value))
			if err != nil {
				return fmt.Errorf("failed to find or create index: %w", err)
			}
			switch value := value.(type) {
			case string:
				if err := page.BPTree(r).Insert(btree.ReferencedValue{
					DataPointer: btree.MemoryPointer{
						Offset: data.Offset + uint64(fieldOffset) + 1,
						Length: uint32(dec.InputOffset()-fieldOffset) - 2,
					},
					// trim the quotes
					Value: []byte(value),
				}, data); err != nil {
					return fmt.Errorf("failed to insert into b+tree: %w", err)
				}
			case json.Number, float64:
				buf := make([]byte, 8)
				switch value := value.(type) {
				case json.Number:
					f, err := value.Float64()
					if err != nil {
						return fmt.Errorf("failed to parse float: %w", err)
					}
					binary.BigEndian.PutUint64(buf, math.Float64bits(f))
				case float64:
					binary.BigEndian.PutUint64(buf, math.Float64bits(value))
				}
				if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: buf}, data); err != nil {
					return fmt.Errorf("failed to insert into b+tree: %w", err)
				}
			case bool:
				if value {
					if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{1}}, data); err != nil {
						return fmt.Errorf("failed to insert into b+tree: %w", err)
					}
				} else {
					if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{0}}, data); err != nil {
						return fmt.Errorf("failed to insert into b+tree: %w", err)
					}
				}
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
					// find the index to set the field type to unknown.
					if err := handleJSONLObject(f, r, dec, append(path, key), data); err != nil {
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
				// nil values are a bit of a degenerate case, we are essentially using the btree
				// as a set. we store the value as an empty byte slice.
				if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{}}, data); err != nil {
					return fmt.Errorf("failed to insert into b+tree: %w", err)
				}
			default:
				return fmt.Errorf("unexpected type '%T'", value)
			}
		}
	}
	return nil
}
