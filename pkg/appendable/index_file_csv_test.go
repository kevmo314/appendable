package appendable

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kevmo314/appendable/pkg/protocol"
)

func TestAppendDataRowCSV(t *testing.T) {

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
		if j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset != uint64(len("{\"test\":")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].FieldStartByteOffset = %d, want 10", j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset)
		}

		if j.Indexes[0].IndexRecords["test3"][0].DataNumber != 0 {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][0].DataNumber = %d, want 1", j.Indexes[0].IndexRecords["test3"][0].DataNumber)
		}

		// verify byte offset calculation
		if j.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset != uint64(len("header\ntest1\n")+1) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][0].FieldStartByteOffset = %d, want 14", j.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset)
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

	// this test case fails due to the way we measure checksums
	// since CSV's are column-based appending new columns, cause checksums and endbyteoffsets to be mismatched
	t.Run("new index", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader("header1,header2\ntest1,test2\n")})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional index
		if len(j.Indexes) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 2", len(j.Indexes))
		}

		if j.Indexes[1].FieldName != "test2" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2\"", j.Indexes[1].FieldName)
		}

		if j.Indexes[1].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[1].FieldType = %+v, want protocol.FieldTypeString", j.Indexes[1].FieldType)
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
