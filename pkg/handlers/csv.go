package handlers

// import (
// 	"bufio"
// 	"bytes"
// 	"encoding/binary"
// 	"encoding/csv"
// 	"fmt"
// 	"io"
// 	"log/slog"
// 	"math"
// 	"strconv"
// 	"strings"

// 	"github.com/kevmo314/appendable/pkg/appendable"
// 	"github.com/kevmo314/appendable/pkg/btree"
// )

// type CSVHandler struct {
// 	io.ReadSeeker
// }

// var _ appendable.DataHandler = (*CSVHandler)(nil)

// func (c CSVHandler) Format() appendable.Format {
// 	return appendable.FormatCSV
// }

// func (c CSVHandler) Synchronize(f *appendable.IndexFile, df appendable.DataFile) error {
// 	slog.Debug("Starting CSV synchronization")

// 	var headers []string
// 	var err error

// 	metadata, err := f.Metadata()
// 	if err != nil {
// 		return fmt.Errorf("failed to read metadata: %w", err)
// 	}
// 	if _, err := df.Seek(int64(metadata.ReadOffset), io.SeekStart); err != nil {
// 		return fmt.Errorf("failed to seek: %w", err)
// 	}

// 	fromNewIndexFile := false

// 	isHeader := false

// 	isEmpty, err := f.IsEmpty()
// 	if err != nil {
// 		return fmt.Errorf("failed to check if index file is empty: %w", err)
// 	}

// 	if isEmpty {
// 		isHeader = true
// 		fromNewIndexFile = true
// 	} else {
// 		slog.Debug("indexes already exist, not parsing headers")
// 		for _, index := range f.Indexes {
// 			isHeader = false
// 			headers = append(headers, index.FieldName)
// 		}
// 	}

// 	scanner := bufio.NewScanner(df)

// 	for scanner.Scan() {
// 		line := scanner.Bytes()

// 		existingCount := len(f.EndByteOffsets)

// 		// append a data range
// 		var start uint64
// 		if len(f.EndByteOffsets) > 0 {
// 			start = f.EndByteOffsets[existingCount-1]
// 		}

// 		slog.Debug("", slog.Uint64("start", start))

// 		slog.Debug("adding", slog.Any("endbyteoffset", start+uint64(len(line))), slog.Any("line", line))

// 		if isHeader {
// 			slog.Info("Parsing CSV headers")
// 			dec := csv.NewReader(bytes.NewReader(line))
// 			headers, err = dec.Read()
// 			if err != nil {
// 				slog.Error("failed to parse CSV header", "error", err)
// 				return fmt.Errorf("failed to parse CSV header: %w", err)
// 			}
// 			isHeader = false
// 			continue
// 		}

// 		dec := csv.NewReader(bytes.NewReader(line))
// 		slog.Debug("Handling csv", "line", i)

// 		if fromNewIndexFile {

// 			handleCSVLine(f, df, dec, headers, []string{}, btree.MemoryPointer{
// 				Offset: start,
// 				Length: uint32(len(line)),
// 			})
// 		} else {

// 			handleCSVLine(f, df, dec, headers, []string{}, uint64(existingCount)
// 		}

// 		metadata.ReadOffset += uint64(len(line)) + 1 // include the newline
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return fmt.Errorf("failed to scan: %w", err)
// 	}

// 	// update the metadata
// 	if err := f.SetMetadata(metadata); err != nil {
// 		return fmt.Errorf("failed to set metadata: %w", err)
// 	}

// 	slog.Debug("indexes", slog.Any("", f.Indexes))
// 	slog.Debug("Ending CSV synchronization")
// 	slog.Debug("=========")
// 	return nil
// }

// func fieldRankCsvField(fieldValue any) int {
// 	slog.Debug("serialize", slog.Any("fieldValue", fieldValue))
// 	switch fieldValue.(type) {
// 	case nil:
// 		slog.Debug("nil", slog.Any("fieldValue", fieldValue))
// 		return 1
// 	case bool:
// 		slog.Debug("bool", slog.Any("fieldValue", fieldValue))
// 		return 2
// 	case int, int8, int16, int32, int64, float32, float64:
// 		slog.Debug("number", slog.Any("fieldValue", fieldValue))
// 		return 3
// 	case string:
// 		slog.Debug("string", slog.Any("fieldValue", fieldValue))
// 		return 4
// 	default:
// 		panic("unknown type")
// 	}
// }

// func InferCSVField(fieldValue string) (interface{}, appendable.FieldType) {
// 	if fieldValue == "" {
// 		return nil, appendable.FieldTypeNull
// 	}

// 	if i, err := strconv.Atoi(fieldValue); err == nil {

// 		fmt.Printf("\n%v is a integer\n", fieldValue)
// 		return float64(i), appendable.FieldTypeFloat64
// 	}

// 	if f, err := strconv.ParseFloat(fieldValue, 64); err == nil {

// 		fmt.Printf("\n%v is a float\n", fieldValue)
// 		return float64(f), appendable.FieldTypeFloat64
// 	}

// 	if b, err := strconv.ParseBool(fieldValue); err == nil {
// 		return b, appendable.FieldTypeBoolean
// 	}

// 	return fieldValue, appendable.FieldTypeString
// }

// func handleCSVLine(f *appendable.IndexFile, r io.ReaderAt, dec *csv.Reader, headers []string, path []string, data btree.MemoryPointer) error {
// 	slog.Debug("Processing CSV line", slog.Int("dataIndex", int(dataIndex)), slog.Int("dataOffset", int(dataOffset)))

// 	record, err := dec.Read()

// 	if err != nil {
// 		slog.Error("Failed to read CSV record at index", "dataIndex", dataIndex, "error", err)
// 		return fmt.Errorf("failed to read CSV record at index %d: %w", dataIndex, err)
// 	}

// 	slog.Debug("CSV line read successfully", "record", record)

// 	cumulativeLength := uint64(0)

// 	for fieldIndex, fieldValue := range record {
// 		if fieldIndex >= len(headers) {
// 			slog.Error("Field index is out of bounds with headers", "fieldIndex", fieldIndex, "headers", slog.Any("headers", headers))
// 			return fmt.Errorf("field index %d is out of bounds with header", fieldIndex)
// 		}

// 		fieldName := headers[fieldIndex]
// 		name := strings.Join(append(path, fieldName), ".")

// 		fieldOffset := data.Offset + cumulativeLength
// 		fieldLength := uint32(len(fieldValue))

// 		value, fieldType := InferCSVField(fieldValue)

// 		page, err := f.FindOrCreateIndex(name, fieldType)
// 		if err != nil {
// 			return fmt.Errorf("failed to find or create index: %w", err)
// 		}

// 		switch fieldType {
// 		case appendable.FieldTypeFloat64:
// 			buf := make([]byte, 8)
// 			binary.LittleEndian.PutUint64(buf, math.Float64bits(value.(float64)))
// 			if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: buf}, data); err != nil {
// 				return fmt.Errorf("failed to insert into b+tree: %w", err)
// 			}
// 		case appendable.FieldTypeBoolean:
// 			if value.(bool) {
// 				if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{1}}, data); err != nil {
// 					return fmt.Errorf("failed to insert into b+tree: %w", err)
// 				}
// 			} else {
// 				if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{0}}, data); err != nil {
// 					return fmt.Errorf("failed to insert into b+tree: %w", err)
// 				}
// 			}
// 		case appendable.FieldTypeString:
// 			if err := page.BPTree(r).Insert(btree.ReferencedValue{
// 				DataPointer: btree.MemoryPointer{
// 					Offset: fieldOffset,
// 					Length: fieldLength,
// 				},
// 				// trim the quotes
// 				Value: []byte(value.(string)),
// 			}, data); err != nil {
// 				return fmt.Errorf("failed to insert into b+tree: %w", err)
// 			}

// 			slog.Debug("Appended index record",
// 				slog.String("field", name),
// 				slog.Any("value", value),
// 				slog.Int("start", int(fieldOffset)))

// 		case appendable.FieldTypeNull:
// 			// nil values are a bit of a degenerate case, we are essentially using the btree
// 			// as a set. we store the value as an empty byte slice.
// 			if err := page.BPTree(r).Insert(btree.ReferencedValue{Value: []byte{}}, data); err != nil {
// 				return fmt.Errorf("failed to insert into b+tree: %w", err)
// 			}
// 			slog.Debug("Marked field", "name", name)

// 		default:
// 			slog.Error("Encountered unexpected type '%T' for field '%s'", value, name)
// 			return fmt.Errorf("unexpected type '%T'", value)
// 		}

// 		cumulativeLength += uint64(fieldLength + 1)
// 	}

// 	return nil
// }
