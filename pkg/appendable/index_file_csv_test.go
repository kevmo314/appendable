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

func TestAppendDataRowCSV(t *testing.T) {

	originalLogger := slog.Default()

	// Create a logger with Debug on
	debugLevel := &slog.LevelVar{}
	debugLevel.Set(slog.LevelDebug)
	debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: debugLevel,
	}))

	slog.SetDefault(debugLogger)

	defer slog.SetDefault(originalLogger)

	var mockCsv string = "header1\ntest1\n"
	var mockCsv2 string = "header1\ntest1\ntest3\n"

	t.Run("no schema changes", func(t *testing.T) {

		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(i.Indexes))
		}

		if len(j.EndByteOffsets) != 2 {
			t.Errorf("got len(i.DataRanges) = %d, want 2", len(i.EndByteOffsets))
		}

		// check that the first data range is untouched despite being incorrect
		if j.EndByteOffsets[0] != uint64(len(mockCsv)) {
			t.Errorf("got i.DataRanges[0].EndByteOffset = %d, want %d", j.EndByteOffsets[0], uint64(len(mockCsv)))
		}

		// check that the second data range has properly set offsets
		if j.EndByteOffsets[1] != uint64(len(mockCsv2)) {
			t.Errorf("got i.DataRanges[1].EndByteOffset = %d, want %d", j.EndByteOffsets[1], uint64(len(mockCsv2)))
		}
	})

	t.Run("check end + start byte offsets multiple", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})
		if err != nil {
			t.Fatal(err)
		}

		if len(i.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(i.Indexes))
		}

		if len(i.Indexes[0].IndexRecords) != 2 {
			t.Errorf("got len(i.Indexes[0].IndexRecords) = %d, want 2", len(i.Indexes[0].IndexRecords))
		}

		if i.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset != uint64(len("header1\n")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].FieldStartByteOffset = %d, want 7", i.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset)
		}

		if i.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset != uint64(len("header1\ntest1\n")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][0].FieldStartByteOffset = %d, want %d", i.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset, uint64(len("header\ntest1\n")))
		}

	})

	t.Run("append index to existing", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(j.Indexes) = %d, want 1", len(j.Indexes))
		}

		if len(j.Indexes[0].IndexRecords) != 2 {
			fmt.Printf("index records look like %v", j.Indexes[0].IndexRecords)
			t.Errorf("got len(j.Indexes[0].IndexRecords) = %d, want 2", len(j.Indexes[0].IndexRecords))
		}

		if len(j.Indexes[0].IndexRecords["test1"]) != 1 {
			t.Errorf("got len(j.Indexes[0].IndexRecords[\"test1\"]) = %d, want 1", len(j.Indexes[0].IndexRecords["test1"]))
		}
		if len(j.Indexes[0].IndexRecords["test3"]) != 1 {
			for key, records := range j.Indexes[0].IndexRecords {
				t.Errorf("\n\n\nKey: %v, Records: %+v", key, records)
			}
			t.Errorf("got len(j.Indexes[0].IndexRecords[\"test3\"]) = %d, want 1", len(j.Indexes[0].IndexRecords["test3"]))
		}

		if j.Indexes[0].IndexRecords["test1"][0].DataNumber != 0 {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].DataNumber = %d, want 0", j.Indexes[0].IndexRecords["test1"][0].DataNumber)
		}
		if j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset != uint64(len("header1\n")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].FieldStartByteOffset = %d, want %d", j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset, uint64(len("header\n")))
		}

		if j.Indexes[0].IndexRecords["test3"][0].DataNumber != 1 {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][0].DataNumber = %d, want 1", j.Indexes[0].IndexRecords["test3"][0].DataNumber)
		}

		// verify byte offset calculation
		if j.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset != uint64(len("header1\ntest1\n")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][0].FieldStartByteOffset = %d, want %d", j.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset, uint64(len("header\ntest1\n")))
		}
	})

	t.Run("assert correct types", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader("n1,n2\n3.4,3\n")})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader("n1,n2\n3.4,3\n4.3,4")})
		if err != nil {
			t.Fatal(err)
		}

		for _, index := range i.Indexes {
			for key := range index.IndexRecords {
				keyType := reflect.TypeOf(key).String()
				if keyType != "float64" {
					t.Errorf("i keytype is %v", keyType)
				}

				if index.FieldType != protocol.FieldTypeNumber {
					t.Errorf("index field type is not number. actual: %v", index.FieldType)
				}
			}
		}

		for _, index := range j.Indexes {
			for key := range index.IndexRecords {
				keyType := reflect.TypeOf(key).String()
				if keyType != "float64" {
					t.Errorf("j keytype is %v", keyType)
				}

				if index.FieldType != protocol.FieldTypeNumber {
					t.Errorf("index field type is not number. actual: %v", index.FieldType)
				}
			}
		}

	})

	t.Run("multiple headers", func(t *testing.T) {

		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader("name,move\nmica,coyote\n")})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader("name,move\nmica,coyote\ngalvao,mount\n")})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 2", len(i.Indexes))
		}

		if len(j.EndByteOffsets) != 2 {
			t.Errorf("got len(i.DataRanges) = %d, want 2", len(i.EndByteOffsets))
		}

		// check that the first data range is untouched despite being incorrect
		if j.EndByteOffsets[0] != uint64(len("name,move\nmica,coyote\n")) {
			t.Errorf("got i.DataRanges[0].EndByteOffset = %d, want %d", j.EndByteOffsets[0], uint64(len("name,move\nmica,coyote\n")))
		}

		// check that the second data range has properly set offsets
		if j.EndByteOffsets[1] != uint64(len("name,move\nmica,coyote\ngalvao,mount\n")) {
			t.Errorf("got i.DataRanges[1].EndByteOffset = %d, want %d", j.EndByteOffsets[1], uint64(len("name,move\nmica,coyote\ngalvao,mount\n")))
		}

		fmt.Printf("index file looks like: %v", j.Indexes)
	})

	t.Run("generate index file", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader("")})

		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		_, err = ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv2)})
		if err != nil {
			t.Fatal(err)
		}

	})

	t.Run("existing index but different type", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader("test\ntest1\n")})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader("test\ntest1\n123\n")})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeString|protocol.FieldTypeNumber {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", j.Indexes[0].FieldType)
		}
	})

	t.Run("existing index but nullable type", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader("test,test2\nomoplata,armbar\n")})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader("test,test2\nomoplata,armbar\n,singlelegx\n")})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 2", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeNull|protocol.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeNullableString", j.Indexes[0].FieldType)
		}
	})

}
