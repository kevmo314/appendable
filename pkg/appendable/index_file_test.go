package appendable

import (
	"bytes"
	"testing"

	"github.com/google/btree"
	"github.com/kevmo314/appendable/pkg/protocol"
)

func TestAppendDataRow(t *testing.T) {
	t.Run("no schema changes", func(t *testing.T) {
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

		_, err := i.AppendDataRow(bytes.NewBufferString("{\"test\":\"test3\"}"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(i.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(i.Indexes))
		}

		if len(i.DataRanges) != 2 {
			t.Errorf("got len(i.DataRanges) = %d, want 2", len(i.Indexes))
		}

		// check that the first data range is untouched despite being incorrect
		if i.DataRanges[0].StartByteOffset != 10 {
			t.Errorf("got i.DataRanges[0].StartByteOffset = %d, want 10", i.DataRanges[0].StartByteOffset)
		}
		if i.DataRanges[0].EndByteOffset != 20 {
			t.Errorf("got i.DataRanges[0].EndByteOffset = %d, want 20", i.DataRanges[0].EndByteOffset)
		}

		// check that the second data range has properly set offsets
		if i.DataRanges[1].StartByteOffset != 21 {
			t.Errorf("got i.DataRanges[1].StartByteOffset = %d, want 21", i.DataRanges[1].StartByteOffset)
		}
		if i.DataRanges[1].EndByteOffset != 21+uint64(len("{\"test\":\"test3\"}")-1) {
			t.Errorf("got i.DataRanges[1].EndByteOffset = %d, want %d", i.DataRanges[1].EndByteOffset, 21+uint64(len("{\"test\":\"test3\"}")-1))
		}
	})

	t.Run("new index", func(t *testing.T) {
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

		_, err := i.AppendDataRow(bytes.NewBufferString("{\"test2\":\"test3\"}"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional index
		if len(i.Indexes) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 2", len(i.Indexes))
		}

		if i.Indexes[1].FieldName != "test2" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2\"", i.Indexes[1].FieldName)
		}

		if i.Indexes[1].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[1].FieldType = %+v, want protocol.FieldTypeString", i.Indexes[1].FieldType)
		}
	})

	t.Run("existing index but different type", func(t *testing.T) {
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

		_, err := i.AppendDataRow(bytes.NewBufferString("{\"test\":123}"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(i.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(i.Indexes))
		}

		if i.Indexes[0].FieldType != protocol.FieldTypeUnknown {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", i.Indexes[0].FieldType)
		}
	})

	t.Run("creates nested indices", func(t *testing.T) {
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

		_, err := i.AppendDataRow(bytes.NewBufferString("{\"test2\":{\"a\":1,\"b\":\"2\"}}"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(i.Indexes) != 3 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(i.Indexes))
		}

		if i.Indexes[0].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", i.Indexes[0].FieldType)
		}

		if i.Indexes[1].FieldType != protocol.FieldTypeNumber {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want protocol.FieldTypeNumber", i.Indexes[1].FieldType)
		}

		if i.Indexes[2].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[2].FieldType = %#v, want protocol.FieldTypeString", i.Indexes[2].FieldType)
		}

		if i.Indexes[0].FieldName != "test" {
			t.Errorf("got i.Indexes[0].FieldName = %s, want \"test\"", i.Indexes[0].FieldName)
		}

		if i.Indexes[1].FieldName != "test2.a" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2.a\"", i.Indexes[1].FieldName)
		}

		if i.Indexes[2].FieldName != "test2.b" {
			t.Errorf("got i.Indexes[2].FieldName = %s, want \"test2.b\"", i.Indexes[2].FieldName)
		}
	})

	t.Run("ignores arrays", func(t *testing.T) {
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

		_, err := i.AppendDataRow(bytes.NewBufferString("{\"test2\":[[1,2,3],4]}"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(i.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(i.Indexes))
		}
	})
}
