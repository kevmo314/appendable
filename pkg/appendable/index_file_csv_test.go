package appendable

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kevmo314/appendable/pkg/protocol"
)

func TestAppendDataRowCSV(t *testing.T) {

	var mockCsv string = "word,length\nhowdy,5"
	var mockCsv2 string = "word,length\nhowdy,5\ngood lord,9"

	t.Run("generate index file", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})

		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		_, err = ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

	})

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
			t.Errorf("got len(i.DataRanges) = %d, want 2", len(i.Indexes))
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

	t.Run("correctly sets field offset", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, JSONLHandler{ReadSeeker: strings.NewReader(mockCsv2)})
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
			t.Errorf("got len(j.Indexes[0].IndexRecords[\"test3\"]) = %d, want 1", len(j.Indexes[0].IndexRecords["test3"]))
		}

		if j.Indexes[0].IndexRecords["test1"][0].DataNumber != 0 {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].DataNumber = %d, want 0", j.Indexes[0].IndexRecords["test1"][0].DataNumber)
		}
		if j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset != uint64(len("{\"test\":")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test1\"][0].FieldStartByteOffset = %d, want 10", j.Indexes[0].IndexRecords["test1"][0].FieldStartByteOffset)
		}

		if j.Indexes[0].IndexRecords["test3"][0].DataNumber != 1 {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][1].DataNumber = %d, want 1", j.Indexes[0].IndexRecords["test3"][1].DataNumber)
		}
		if j.Indexes[0].IndexRecords["test3"][0].FieldStartByteOffset != uint64(len("{\"test\":\"test1\"}\n{\"test\":")) {
			t.Errorf("got i.Indexes[0].IndexRecords[\"test3\"][1].FieldStartByteOffset = %d, want 10", j.Indexes[0].IndexRecords["test3"][1].FieldStartByteOffset)
		}
	})

	t.Run("new index", func(t *testing.T) {
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

}
