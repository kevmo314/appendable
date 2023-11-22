package appendable

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kevmo314/appendable/pkg/protocol"
)

func TestAppendDataRow(t *testing.T) {
	t.Run("no schema changes", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test\":\"test3\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(i.Indexes))
		}

		if len(j.DataRanges) != 2 {
			t.Errorf("got len(i.DataRanges) = %d, want 2", len(i.Indexes))
		}

		// check that the first data range is untouched despite being incorrect
		if j.DataRanges[0].EndByteOffset != uint64(len("{\"test\":\"test1\"}")) {
			t.Errorf("got i.DataRanges[0].EndByteOffset = %d, want %d", j.DataRanges[0].EndByteOffset, uint64(len("{\"test\":\"test1\"}")))
		}

		// check that the second data range has properly set offsets
		if j.DataRanges[1].EndByteOffset != uint64(len("{\"test\":\"test1\"}\n{\"test\":\"test3\"}")) {
			t.Errorf("got i.DataRanges[1].EndByteOffset = %d, want %d", j.DataRanges[1].EndByteOffset, uint64(len("{\"test\":\"test1\"}\n{\"test\":\"test3\"}")))
		}
	})

	t.Run("new index", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test2\":\"test3\"}\n"))
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
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test\":123}\n"))
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

	t.Run("creates nested indices", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test2\":{\"a\":1,\"b\":\"2\"}}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 3 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", j.Indexes[0].FieldType)
		}

		if j.Indexes[1].FieldType != protocol.FieldTypeNumber {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want protocol.FieldTypeNumber", j.Indexes[1].FieldType)
		}

		if j.Indexes[2].FieldType != protocol.FieldTypeString {
			t.Errorf("got i.Indexes[2].FieldType = %#v, want protocol.FieldTypeString", j.Indexes[2].FieldType)
		}

		if j.Indexes[0].FieldName != "test" {
			t.Errorf("got i.Indexes[0].FieldName = %s, want \"test\"", j.Indexes[0].FieldName)
		}

		if j.Indexes[1].FieldName != "test2.a" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2.a\"", j.Indexes[1].FieldName)
		}

		if j.Indexes[2].FieldName != "test2.b" {
			t.Errorf("got i.Indexes[2].FieldName = %s, want \"test2.b\"", j.Indexes[2].FieldName)
		}
	})

	t.Run("creates nested indices but also erases parent", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test\":{\"a\":1,\"b\":\"2\"}}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 3 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeString|protocol.FieldTypeObject {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", j.Indexes[0].FieldType)
		}
	})

	t.Run("ignores arrays", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test2\":[[1,2,3],4]}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(j.Indexes))
		}
	})

	t.Run("ignores arrays but downgrades type", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test\":[[1,2,3],4]}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 3", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeString|protocol.FieldTypeArray {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeUnknown", j.Indexes[0].FieldType)
		}
	})

	t.Run("existing index but nullable type", func(t *testing.T) {
		i, err := NewIndexFile(strings.NewReader("{\"test\":\"test1\"}\n"))
		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		j, err := ReadIndexFile(buf, strings.NewReader("{\"test\":\"test1\"}\n{\"test\":null}\n"))
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(j.Indexes) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(j.Indexes))
		}

		if j.Indexes[0].FieldType != protocol.FieldTypeNull|protocol.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want protocol.FieldTypeNullableString", j.Indexes[0].FieldType)
		}
	})
}
