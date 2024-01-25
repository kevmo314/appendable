package appendable

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/kevmo314/appendable/pkg/protocol"
)

type CSVHandler struct {
	io.ReadSeeker
}

func (c CSVHandler) Synchronize(f *IndexFile) error {
	slog.Debug("Starting CSV synchronization")

	var headers []string
	var err error

	fromNewIndexFile := false

	isHeader := false

	if len(f.Indexes) == 0 {
		isHeader = true
		fromNewIndexFile = true
	} else {
		slog.Debug("indexes already exist, not parsing headers")
		for _, index := range f.Indexes {
			isHeader = false
			headers = append(headers, index.FieldName)
		}
	}

	scanner := bufio.NewScanner(f.data)

	for i := 0; scanner.Scan(); i++ {
		line := scanner.Bytes()

		existingCount := len(f.EndByteOffsets)

		// append a data range
		var start uint64
		if len(f.EndByteOffsets) > 0 {
			start = f.EndByteOffsets[existingCount-1]
		}

		slog.Debug("", slog.Uint64("start", start))

		slog.Debug("adding", slog.Any("endbyteoffset", start+uint64(len(line))), slog.Any("line", line))
		f.EndByteOffsets = append(f.EndByteOffsets, start+uint64(len(line))+1)
		f.Checksums = append(f.Checksums, xxhash.Sum64(line))

		if isHeader {
			slog.Info("Parsing CSV headers")
			dec := csv.NewReader(bytes.NewReader(line))
			headers, err = dec.Read()
			if err != nil {
				slog.Error("failed to parse CSV header", "error", err)
				return fmt.Errorf("failed to parse CSV header: %w", err)
			}
			isHeader = false
			continue
		}

		dec := csv.NewReader(bytes.NewReader(line))
		slog.Debug("Handling csv", "line", i)

		if fromNewIndexFile {

			f.handleCSVLine(dec, headers, []string{}, uint64(existingCount)-1, start)
		} else {

			f.handleCSVLine(dec, headers, []string{}, uint64(existingCount), start)
		}

		slog.Info("Succesfully processed", "line", i)
	}

	if fromNewIndexFile && len(f.EndByteOffsets) > 0 {
		f.EndByteOffsets = f.EndByteOffsets[1:]
		f.Checksums = f.Checksums[1:]

		slog.Debug("Trimming endbyte offsets and checksums", "endByteOffsets", slog.Any("endByteOffsets", f.EndByteOffsets), "checksums", slog.Any("checksums", f.Checksums))
	}

	slog.Debug("indexes", slog.Any("", f.Indexes))
	slog.Debug("Ending CSV synchronization")
	slog.Debug("=========")
	return nil
}

func fieldRankCsvField(fieldValue any) int {
	switch fieldValue.(type) {
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

func inferCSVField(fieldValue string) (interface{}, protocol.FieldType) {
	if fieldValue == "" {
		return nil, protocol.FieldTypeNull
	}

	if i, err := strconv.Atoi(fieldValue); err == nil {
		return i, protocol.FieldTypeNumber
	}

	if f, err := strconv.ParseFloat(fieldValue, 64); err == nil {
		return f, protocol.FieldTypeNumber
	}

	if b, err := strconv.ParseBool(fieldValue); err == nil {
		return b, protocol.FieldTypeBoolean
	}

	return fieldValue, protocol.FieldTypeString
}

func (i *IndexFile) handleCSVLine(dec *csv.Reader, headers []string, path []string, dataIndex, dataOffset uint64) error {
	slog.Debug("Processing CSV line", slog.Int("dataIndex", int(dataIndex)), slog.Int("dataOffset", int(dataOffset)))

	record, err := dec.Read()

	if err != nil {
		slog.Error("Failed to read CSV record at index", "dataIndex", dataIndex, "error", err)
		return fmt.Errorf("failed to read CSV record at index %d: %w", dataIndex, err)
	}

	slog.Debug("CSV line read successfully", "record", record)

	cumulativeLength := uint64(0)

	for fieldIndex, fieldValue := range record {
		if fieldIndex >= len(headers) {
			slog.Error("Field index is out of bounds with headers", "fieldIndex", fieldIndex, "headers", slog.Any("headers", headers))
			return fmt.Errorf("field index %d is out of bounds with header", fieldIndex)
		}

		fieldName := headers[fieldIndex]
		name := strings.Join(append(path, fieldName), ".")

		fieldOffset := dataOffset + cumulativeLength
		fieldLength := uint64(len(fieldValue))

		value, fieldType := inferCSVField(fieldValue)

		switch fieldType {
		case protocol.FieldTypeBoolean, protocol.FieldTypeString, protocol.FieldTypeNumber:
			tree := i.Indexes[i.findIndex(name, value)].IndexRecords

			tree[value] = append(tree[value], protocol.IndexRecord{
				DataNumber:           dataIndex,
				FieldStartByteOffset: uint64(fieldOffset),
				FieldLength:          int(fieldLength),
			})

			slog.Debug("Appended index record",
				slog.String("field", name),
				slog.Any("value", value),
				slog.Int("start", int(fieldOffset)))

		case protocol.FieldTypeNull:
			for j := range i.Indexes {
				if i.Indexes[j].FieldName == name {
					i.Indexes[j].FieldType |= protocol.FieldTypeNull
				}
			}
			slog.Debug("Marked field", "name", name)

		default:
			slog.Error("Encountered unexpected type '%T' for field '%s'", value, name)
			return fmt.Errorf("unexpected type '%T'", value)
		}

		cumulativeLength += fieldLength + 1
	}

	return nil
}
