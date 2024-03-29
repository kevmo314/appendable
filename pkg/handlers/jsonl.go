package handlers

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/kevmo314/appendable/pkg/pointer"
	"github.com/kevmo314/appendable/pkg/trigram"
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

func (j JSONLHandler) Synchronize(f *appendable.IndexFile, df []byte) error {
	// read until the next newline
	metadata, err := f.Metadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	for {
		i := bytes.IndexByte(df[metadata.ReadOffset:], '\n')
		if i == -1 {
			break
		}
		// create a new json decoder
		dec := json.NewDecoder(bytes.NewReader(df[metadata.ReadOffset:(metadata.ReadOffset + uint64(i))]))

		// if the first token is not {, then return an error
		if t, err := dec.Token(); err != nil || t != json.Delim('{') {
			return fmt.Errorf("expected '%U', got '%U' (only json objects are supported at the root)", '{', t)
		}

		if err := j.handleJSONLObject(f, df, dec, []string{}, pointer.MemoryPointer{
			Offset: metadata.ReadOffset,
			Length: uint32(i),
		}); err != nil {
			return fmt.Errorf("failed to handle object: %w", err)
		}

		// the next token must be a }
		if t, err := dec.Token(); err != nil || t != json.Delim('}') {
			return fmt.Errorf("expected '}', got '%v'", t)
		}

		metadata.ReadOffset += uint64(i) + 1 // include the newline

		if f.BenchmarkCallback != nil {
			f.BenchmarkCallback(int(metadata.ReadOffset))
		}
	}

	// update the metadata
	if err := f.SetMetadata(metadata); err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	return nil
}

func jsonTypeToFieldType(t json.Token) []appendable.FieldType {
	switch t.(type) {
	case json.Delim:
		switch t {
		case json.Delim('{'):
			return []appendable.FieldType{appendable.FieldTypeObject}
		case json.Delim('['):
			return []appendable.FieldType{appendable.FieldTypeArray}
		}
	case json.Number, float64:
		return []appendable.FieldType{appendable.FieldTypeFloat64}
	case string:
		return []appendable.FieldType{appendable.FieldTypeString, appendable.FieldTypeTrigram}
	case bool:
		return []appendable.FieldType{appendable.FieldTypeBoolean}
	case nil:
		return []appendable.FieldType{appendable.FieldTypeNull}
	}
	panic(fmt.Sprintf("unexpected token '%v'", t))
}

func (j JSONLHandler) Parse(value []byte) []byte {
	token, err := json.NewDecoder(bytes.NewReader(value)).Token()
	if err != nil {
		slog.Error("failed to parse token", "err", err)
		return nil
	}
	switch token := token.(type) {
	case string:
		return []byte(token)
	case json.Number, float64:
		buf := make([]byte, 8)
		switch token := token.(type) {
		case json.Number:
			f, err := token.Float64()
			if err != nil {
				slog.Error("failed to parse float", "err", err)
				return nil
			}
			binary.BigEndian.PutUint64(buf, math.Float64bits(f))
		case float64:
			binary.BigEndian.PutUint64(buf, math.Float64bits(token))
		}
		return buf
	case bool:
		if token {
			return []byte{1}
		}
		return []byte{0}
	case json.Delim:
		panic("unexpected delimiter, objects should not be indexed!")
	case nil:
		return []byte{}
	}
	panic(fmt.Sprintf("unexpected token '%v'", token))
}

func (j JSONLHandler) handleJSONLObject(f *appendable.IndexFile, r []byte, dec *json.Decoder, path []string, data pointer.MemoryPointer) error {
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

			for _, ft := range jsonTypeToFieldType(value) {
				page, meta, err := f.FindOrCreateIndex(name, ft)
				if err != nil {
					return fmt.Errorf("failed to find or create index: %w", err)
				}
				width := meta.Width
				mp := pointer.MemoryPointer{
					Offset: data.Offset + uint64(fieldOffset),
					Length: uint32(dec.InputOffset() - fieldOffset),
				}

				switch ft {
				case appendable.FieldTypeString:
					valueStr, ok := value.(string)
					if !ok {
						return fmt.Errorf("expected string")
					}
					valueBytes := []byte(valueStr)

					if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
						DataPointer: mp,
						Value:       valueBytes,
					}, data); err != nil {
						return fmt.Errorf("failed to insert into b+tree: %w", err)
					}
				case appendable.FieldTypeTrigram:
					valueStr, ok := value.(string)
					if !ok {
						return fmt.Errorf("expected string")
					}
					trigrams := trigram.BuildTrigram(valueStr)

					for _, tri := range trigrams {
						valueBytes := []byte(tri.Word)
						mp.Offset += tri.Offset
						mp.Length += tri.Length
						if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
							DataPointer: mp,
							Value:       valueBytes,
						}, data); err != nil {
							return fmt.Errorf("failed to insert into b+tree: %w", err)
						}
					}
				case appendable.FieldTypeNull:
					// nil values are a bit of a degenerate case, we are essentially using the btree
					// as a set. we store the value as an empty byte slice.
					if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
						Value:       []byte{},
						DataPointer: mp,
					}, data); err != nil {
						return fmt.Errorf("failed to insert into b+tree: %w\nmp: %v", err, data.Offset)
					}
				case appendable.FieldTypeFloat64, appendable.FieldTypeUint64, appendable.FieldTypeInt64:
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

					if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
						DataPointer: mp,
						Value:       buf,
					},
						data); err != nil {
						return fmt.Errorf("failed to insert into b+tree: %w", err)
					}
				case appendable.FieldTypeBoolean:
					valueBool, ok := value.(bool)
					if !ok {
						return fmt.Errorf("expected bool type")
					}
					if valueBool {
						if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
							DataPointer: mp,
							Value:       []byte{1},
						}, data); err != nil {
							return fmt.Errorf("failed to insert into b+tree: %w", err)
						}
					} else {
						if err := page.BPTree(&btree.BPTree{Data: r, DataParser: j, Width: width}).Insert(btree.ReferencedValue{
							DataPointer: mp,
							Value:       []byte{0},
						}, data); err != nil {
							return fmt.Errorf("failed to insert into b+tree: %w", err)
						}
					}
				case appendable.FieldTypeArray, appendable.FieldTypeObject:
					switch value := value.(type) {
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
							if err := j.handleJSONLObject(f, r, dec, append(path, key), data); err != nil {
								return fmt.Errorf("failed to handle object: %w", err)
							}
							// read the }
							if t, err := dec.Token(); err != nil || t != json.Delim('}') {
								return fmt.Errorf("expected '}', got '%v'", t)
							}
						}
					}

				default:
					return fmt.Errorf("unrecognized type: %T: %v", ft, ft)
				}

			}
		}
	}
	return nil
}
