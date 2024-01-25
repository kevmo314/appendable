package appendable

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/kevmo314/appendable/pkg/protocol"
)

/*
This test file performs deep checks between two Index files.

When introducing a new file format, this testing file serves to check if the index file from the newly supported file format is identical to index files from currently supported file formats.

We'll use the green_tripdata_2023-01 dataset as our input

Current findings when comparing:
jsonl <---> csv
> the field length doesn't align, it seems like JSONL is accounting for "" for strings, while CSV measures raw string values
*/
func TestIndexFile(t *testing.T) {

	originalLogger := slog.Default()

	// Create a logger with Debug on
	debugLevel := &slog.LevelVar{}
	debugLevel.Set(slog.LevelDebug)
	debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: debugLevel,
	}))

	slog.SetDefault(debugLogger)

	defer slog.SetDefault(originalLogger)

	mockJsonl := "{\"h1\":\"test1\", \"h2\":37.3}\n"
	mockJsonl2 := "{\"h1\":\"test1\", \"h2\":37.3}\n{\"h1\":\"test3\", \"h2\":4}\n"

	mockCsv := "h1,h2\ntest1,37.3\n"
	mockCsv2 := "h1,h2\ntest1,37.3\ntest3,4\n"

	t.Run("compare mock index file", func(t *testing.T) {
		// jsonl
		jif, err := NewIndexFile(JSONLHandler{ReadSeeker: strings.NewReader(mockJsonl2)})

		if err != nil {
			t.Fatal(err)
		}

		civ, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})

		if err != nil {
			t.Fatal(err)
		}

		status, res := jif.compareTo(civ)

		if !status {
			t.Errorf("Not equal\n%v", res)
		}

	})

	t.Run("compare mock index file after appending", func(t *testing.T) {
		jif, err := NewIndexFile(JSONLHandler{ReadSeeker: strings.NewReader(mockJsonl)})
		if err != nil {
			t.Fatal(err)
		}

		jbuf := &bytes.Buffer{}

		if err := jif.Serialize(jbuf); err != nil {
			t.Fatal(err)
		}

		civ, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

		cbuf := &bytes.Buffer{}
		if err := civ.Serialize(cbuf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(jbuf, JSONLHandler{ReadSeeker: strings.NewReader(mockJsonl2)})
		if err != nil {
			t.Fatal(err)
		}

		c, err := ReadIndexFile(cbuf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})
		if err != nil {
			t.Fatal(err)
		}
		status, res := j.compareTo(c)

		fmt.Printf("%v", c)

		if !status {
			t.Errorf("Not equal\n%v", res)
		}

	})

}

func compareIndexRecord(ir1, ir2 *protocol.IndexRecord, fieldType protocol.FieldType) (bool, string) {
	if ir1.DataNumber != ir2.DataNumber {
		return false, fmt.Sprintf("Index record data numbers do not align\ti1: %v, i2: %v", ir1.DataNumber, ir2.DataNumber)
	}

	if fieldType&protocol.FieldTypeString != protocol.FieldTypeString {
		if ir1.FieldLength != ir2.FieldLength {
			return false, fmt.Sprintf("Field Length do not align\ti1: %v, i2: %v", ir1.FieldLength, ir2.FieldLength)
		}
	} else {
		if ir1.FieldLength != ir2.FieldLength+2 {
			return false, fmt.Sprintf("Field Length do not align\ti1: %v, i2: %v", ir1.FieldLength, ir2.FieldLength)
		}
	}
	return true, ""
}

func (i1 *Index) compareIndex(i2 *Index) (bool, string) {
	// compare fieldname
	if i1.FieldName != i2.FieldName {
		return false, fmt.Sprintf("field names do not align\ti1: %v, i2: %v", i1.FieldName, i2.FieldName)
	}

	// compare fieldtype
	if i1.FieldType != i2.FieldType {
		return false, fmt.Sprintf("field types do not align\ti1: %v, i2: %v", i1.FieldType, i2.FieldType)
	}

	// compare index records
	if len(i1.IndexRecords) != len(i2.IndexRecords) {
		return false, fmt.Sprintf("index record lengths do not line up\ti1: %v, i2: %v", len(i1.IndexRecords), len(i2.IndexRecords))
	}

	for key, records1 := range i1.IndexRecords {
		records2, ok := i2.IndexRecords[key]

		var keysAndTypesI1, keysAndTypesI2 []string

		// Iterate through i1.IndexRecords to collect keys and their types
		for key := range i1.IndexRecords {
			keyType := reflect.TypeOf(key).String() // Get the type of the key as a string
			keysAndTypesI1 = append(keysAndTypesI1, fmt.Sprintf("%v (%s)", key, keyType))
		}

		// Iterate through i2.IndexRecords to collect keys and their types
		for key := range i2.IndexRecords {
			keyType := reflect.TypeOf(key).String() // Get the type of the key as a string
			keysAndTypesI2 = append(keysAndTypesI2, fmt.Sprintf("%v (%s)", key, keyType))
		}

		if !ok {
			return false, fmt.Sprintf("key doesn't exist in i2\tkey found in i1: %v\n%v\t%v\n%v\t%v", key, i1.IndexRecords, i2.IndexRecords, keysAndTypesI1, keysAndTypesI2)
		}

		for i := range records1 {
			status, res := compareIndexRecord(&records1[i], &records2[i], i1.FieldType)
			if !status {
				return false, res
			}
		}
	}

	return true, ""
}

func (i1 *IndexFile) compareTo(i2 *IndexFile) (bool, string) {
	// check versions
	if i1.Version != i2.Version {
		return false, fmt.Sprintf("versions mismatched\ti1: %v, i2: %v", i1.Version, i2.Version)
	}

	if len(i1.Indexes) != len(i2.Indexes) {
		return false, fmt.Sprintf("indexes length not equal\ti1: %v, i2: %v", len(i1.Indexes), len(i2.Indexes))
	}

	for i, index1 := range i1.Indexes {
		index2 := i2.Indexes[i]

		status, res := index1.compareIndex(&index2)

		if !status {
			return false, res
		}
	}

	if len(i1.EndByteOffsets) != len(i2.EndByteOffsets) {
		return false, fmt.Sprintf("endbyteoffsets length not equal\ti1: %v, i2: %v", len(i1.EndByteOffsets), len(i2.EndByteOffsets))
	}

	fmt.Printf("endbyteoffsets equal")

	if len(i1.Checksums) != len(i2.Checksums) {
		return false, fmt.Sprintf("checksums length not equal\ti1: %v, i2: %v", len(i1.Checksums), len(i2.Checksums))
	}

	return true, "great success!"
}
