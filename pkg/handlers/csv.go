package handlers

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
)

type CSVHandler struct {
	io.ReadSeeker
}

var _ appendable.DataHandler = (*CSVHandler)(nil)

func (c CSVHandler) Format() appendable.Format {
	return appendable.FormatCSV
}

func (c CSVHandler) Synchronize(f *appendable.IndexFile, df []byte) error {
	slog.Debug("Starting CSV synchronization")

	var headers []string
	var err error

	metadata, err := f.Metadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	fieldNames, err := f.IndexFieldNames()
	if err != nil {
		return fmt.Errorf("failed to retrieve index field names: %w", err)
	}
	headers = fieldNames

	for {
		i := bytes.IndexByte(df[metadata.ReadOffset:], '\n')
		if i == -1 {
			break
		}

		if len(headers) == 0 {
			slog.Info("Parsing CSV headers")
			dec := csv.NewReader(bytes.NewReader(df[metadata.ReadOffset : metadata.ReadOffset+uint64(i)]))
			headers, err = dec.Read()
			if err != nil {
				slog.Error("failed to parse CSV header", "error", err)
				return fmt.Errorf("failed to parse CSV header: %w", err)
			}
			metadata.ReadOffset += uint64(i) + 1
			continue
		}

		dec := csv.NewReader(bytes.NewReader(df[metadata.ReadOffset : metadata.ReadOffset+uint64(i)]))

		if err := c.handleCSVLine(f, df, dec, headers, []string{}, pointer.MemoryPointer{
			Offset: metadata.ReadOffset,
			Length: uint32(i),
		}); err != nil {
			return fmt.Errorf("failed to handle object: %w", err)
		}

		metadata.ReadOffset += uint64(i) + 1 // include the newline
	}

	// update the metadata
	if err := f.SetMetadata(metadata); err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	slog.Debug("indexes", slog.Any("", f.Indexes))
	slog.Debug("Ending CSV synchronization")
	slog.Debug("=========")
	return nil
}

func fieldRankCsvField(fieldValue any) int {
	slog.Debug("serialize", slog.Any("fieldValue", fieldValue))
	switch fieldValue.(type) {
	case nil:
		slog.Debug("nil", slog.Any("fieldValue", fieldValue))
		return 1
	case bool:
		slog.Debug("bool", slog.Any("fieldValue", fieldValue))
		return 2
	case int, int8, int16, int32, int64, float32, float64:
		slog.Debug("number", slog.Any("fieldValue", fieldValue))
		return 3
	case string:
		slog.Debug("string", slog.Any("fieldValue", fieldValue))
		return 4
	default:
		panic("unknown type")
	}
}

func InferCSVField(fieldValue string) (interface{}, appendable.FieldType) {
	if fieldValue == "" {
		return nil, appendable.FieldTypeNull
	}

	if i, err := strconv.Atoi(fieldValue); err == nil {

		return float64(i), appendable.FieldTypeFloat64
	}

	if f, err := strconv.ParseFloat(fieldValue, 64); err == nil {

		return float64(f), appendable.FieldTypeFloat64
	}

	if b, err := strconv.ParseBool(fieldValue); err == nil {
		return b, appendable.FieldTypeBoolean
	}

	return fieldValue, appendable.FieldTypeString
}

func (c CSVHandler) Parse(value []byte) []byte {
	parsed, fieldType := InferCSVField(string(value))

	switch fieldType {
	case appendable.FieldTypeFloat64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(parsed.(float64)))
		return buf
	case appendable.FieldTypeBoolean:
		if parsed.(bool) {
			return []byte{1}
		} else {
			return []byte{0}
		}
	case appendable.FieldTypeString:
		return []byte(parsed.(string))
	case appendable.FieldTypeNull:
		// nil values are a bit of a degenerate case, we are essentially using the btree
		// as a set. we store the value as an empty byte slice.
		return []byte{}
	}
	panic("unknown type")
}

func (c CSVHandler) handleCSVLine(f *appendable.IndexFile, df []byte, dec *csv.Reader, headers []string, path []string, data pointer.MemoryPointer) error {
	record, err := dec.Read()
	if err != nil {
		slog.Error("Failed to read CSV record at index", "error", err)
		return fmt.Errorf("failed to read CSV record: %w", err)
	}

	cumulativeLength := uint64(0)

	for fieldIndex, fieldValue := range record {
		if fieldIndex >= len(headers) {
			slog.Error("Field index is out of bounds with headers", "fieldIndex", fieldIndex, "headers", slog.Any("headers", headers))
			return fmt.Errorf("field index %d is out of bounds with header", fieldIndex)
		}

		fieldName := headers[fieldIndex]

		name := strings.Join(append(path, fieldName), ".")

		fieldOffset := data.Offset + cumulativeLength
		fieldLength := uint32(len(fieldValue))

		_, fieldType := InferCSVField(fieldValue)
		page, _, err := f.FindOrCreateIndex(name, fieldType)

		if err != nil {
			return fmt.Errorf("failed to find or create index: %w", err)
		}

		mp := pointer.MemoryPointer{
			Offset: fieldOffset,
			Length: fieldLength,
		}

		if err := page.BPTree(&btree.BPTree{Data: df, DataParser: CSVHandler{}, Width: uint16(0)}).Insert(btree.ReferencedValue{Value: c.Parse([]byte(fieldValue)), DataPointer: mp}, data); err != nil {
			return fmt.Errorf("failed to insert into b+tree: %w", err)
		}

		cumulativeLength += uint64(fieldLength + 1)
	}

	return nil
}
