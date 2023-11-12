package appendable

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/btree"
	"github.com/kevmo314/appendable/pkg/protocol"
)

func TestReadIndex(t *testing.T) {
	t.Run("empty index file", func(t *testing.T) {
		i := &IndexFile{}
		if err := ReadIndexFile(strings.NewReader(""), i); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}

func TestWriteIndex(t *testing.T) {
	t.Run("basic index file", func(t *testing.T) {
		i := &IndexFile{
			Version: CurrentVersion,
			Indexes: []Index{
				{
					FieldName:    "test",
					FieldType:    protocol.FieldTypeString,
					IndexRecords: btree.NewG[protocol.IndexRecord](2, nil),
				},
			},
			DataRanges: []protocol.DataRange{
				{
					StartByteOffset: 10,
					EndByteOffset:   20,
				},
			},
		}
		buf := &bytes.Buffer{}
		if err := WriteIndexFile(buf, i); err != nil {
			t.Fatal(err)
		}

		// deserialize the index file
		j := &IndexFile{}
		if err := ReadIndexFile(buf, j); err != nil {
			t.Fatal(err)
		}

		if j.Version != i.Version {
			t.Errorf("expected version %d, got %d", i.Version, j.Version)
		}
		if len(j.Indexes) != len(i.Indexes) {
			t.Errorf("expected %d indexes, got %d", len(i.Indexes), len(j.Indexes))
		}
		if len(j.DataRanges) != len(i.DataRanges) {
			t.Errorf("expected %d data ranges, got %d", len(i.DataRanges), len(j.DataRanges))
		}
		// check the index name
		if j.Indexes[0].FieldName != i.Indexes[0].FieldName {
			t.Errorf("expected index name %q, got %q", i.Indexes[0].FieldName, j.Indexes[0].FieldName)
		}
		// check the index type
		if j.Indexes[0].FieldType != i.Indexes[0].FieldType {
			t.Errorf("expected index type %d, got %d", i.Indexes[0].FieldType, j.Indexes[0].FieldType)
		}
		// check that the btree has one element
		if j.Indexes[0].IndexRecords.Len() != i.Indexes[0].IndexRecords.Len() {
			t.Errorf("expected index record count %d, got %d", i.Indexes[0].IndexRecords.Len(), j.Indexes[0].IndexRecords.Len())
		}
		// check the data range start byte offset
		if j.DataRanges[0].StartByteOffset != i.DataRanges[0].StartByteOffset {
			t.Errorf("expected data range start byte offset %d, got %d", i.DataRanges[0].StartByteOffset, j.DataRanges[0].StartByteOffset)
		}
		// check the data range end byte offset
		if j.DataRanges[0].EndByteOffset != i.DataRanges[0].EndByteOffset {
			t.Errorf("expected data range end byte offset %d, got %d", i.DataRanges[0].EndByteOffset, j.DataRanges[0].EndByteOffset)
		}
	})
}
