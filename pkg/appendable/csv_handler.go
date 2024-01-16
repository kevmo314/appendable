package appendable

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/kevmo314/appendable/pkg/protocol"
)

type CSVHandler struct {
	io.ReadSeeker
}

func (c CSVHandler) Synchronize(f *IndexFile) error {
	var headers []string
	var err error

	isHeader := false

	if len(f.Indexes) == 0 {
		isHeader = true
	} else {
		for _, index := range f.Indexes {
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

		f.EndByteOffsets = append(f.EndByteOffsets, start+uint64(len(line))+1)

		f.Checksums = append(f.Checksums, xxhash.Sum64(line))

		if isHeader {
			dec := csv.NewReader(bytes.NewReader(line))
			headers, err = dec.Read()
			if err != nil {
				return fmt.Errorf("failed to parse CSV header: %w", err)
			}
			isHeader = false
			continue
		}

		dec := csv.NewReader(bytes.NewReader(line))
		f.handleCSVLine(dec, headers, []string{}, uint64(existingCount)-1, start)
	}

	return nil
}

func fieldRankCsvField(fieldValue any) int {
	fieldStr, ok := fieldValue.(string)

	if !ok {
		panic("unknown type")
	}

	if fieldStr == "" {
		return 1
	}

	if _, err := strconv.Atoi(fieldStr); err == nil {
		return 3
	}

	if _, err := strconv.ParseFloat(fieldStr, 64); err == nil {
		return 3
	}

	if _, err := strconv.ParseBool(fieldStr); err == nil {
		return 2
	}

	return 4
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

	record, err := dec.Read()

	if err != nil {
		return fmt.Errorf("failed to read CSV record at index %d: %w", dataIndex, err)
	}

	cumulativeLength := uint64(0)

	for fieldIndex, fieldValue := range record {
		if fieldIndex >= len(headers) {
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

		case protocol.FieldTypeNull:
			for j := range i.Indexes {
				if i.Indexes[j].FieldName == name {
					i.Indexes[j].FieldType |= protocol.FieldTypeNull
				}
			}

		default:
			return fmt.Errorf("unexpected type '%T'", value)
		}

		cumulativeLength += fieldLength
	}

	return nil
}
