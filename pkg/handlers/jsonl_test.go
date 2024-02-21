package handlers

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/buftest"
)

func TestJSONL(t *testing.T) {
	t.Run("no schema changes", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()
		g := []byte("{\"test\":\"test1\"}\n")

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		indexes1, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected1, err := indexes1.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collected1) != 0 {
			t.Errorf("got len(i.Indexes) = %d, want 0", len(collected1))
		}

		if err := i.Synchronize(g); err != nil {
			t.Fatal(err)
		}

		indexes2, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected2, err := indexes2.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collected2) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected2))
		}
	})

	t.Run("correctly sets field offset", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n")); err != nil {
			t.Fatal(err)
		}

		r2 := []byte("{\"test\":\"test1\"}\n{\"test\":\"test3\"}\n")
		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collected) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}

		rv1, mp1, err := collected[0].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}
		if mp1 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != 0 || mp1.Length != uint32(len("{\"test\":\"test1\"}")) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = %+v, want {0, %d}", mp1, len("{\"test\":\"test1\"}"))
		}

		rv2, mp2, err := collected[0].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test3")})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test3\") = nil, want non-nil")
		}

		if !bytes.Equal(rv2.Value, []byte("test3")) {
			t.Errorf("incorrect values, got %v, want %v", rv2.Value, []byte("test3"))
		}

		if mp2.Offset != uint64(len("{\"test\":\"test1\"}\n")) || mp2.Length != uint32(len("{\"test\":\"test3\"}")) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test3\") = %+v, want {%d, %d}", mp2, len("{\"test\":\"test1\"}\n"), len("{\"test\":\"test3\"}"))
		}
	})

	t.Run("new index", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n")); err != nil {
			t.Fatal(err)
		}

		r2 := []byte("{\"test\":\"test1\"}\n{\"test2\":\"test3\"}\n")
		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional index
		if len(collected) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}

		rv1, mp1, err := collected[0].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}
		if mp1 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != 0 || mp1.Length != uint32(len("{\"test\":\"test1\"}")) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = %+v, want {0, %d}", mp1, len("{\"test\":\"test1\"}"))
		}

		buf1, err := collected[0].Metadata()
		if err != nil {
			t.Fatal(err)
		}
		md1 := &appendable.IndexMeta{}
		if err := md1.UnmarshalBinary(buf1); err != nil {
			t.Fatal(err)
		}
		if md1.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md1.FieldType)
		}

		rv2, mp2, err := collected[1].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test3")})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[1].BPTree().Find(\"test3\") = nil, want non-nil")
		}

		if !bytes.Equal(rv2.Value, []byte("test3")) {
			t.Errorf("incorrect values, got %v, want %v", rv2.Value, []byte("test3"))
		}

		if mp2.Offset != uint64(len("{\"test\":\"test1\"}\n")) || mp2.Length != uint32(len("{\"test2\":\"test3\"}")) {
			t.Errorf("got i.Indexes[1].BPTree().Find(\"test3\") = %+v, want {%d, %d}", mp2, len("{\"test\":\"test1\"}\n"), len("{\"test2\":\"test3\"}"))
		}

		md2 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md2); err != nil {
			t.Fatal(err)
		}
		if md2.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeString", md2.FieldType)
		}
	})

	t.Run("existing index but different type", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n")); err != nil {
			t.Fatal(err)
		}

		r2 := []byte("{\"test\":\"test1\"}\n{\"test\":123}\n")
		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional index
		if len(collected) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}

		rv1, mp1, err := collected[0].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}
		if mp1 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != 0 || mp1.Length != uint32(len("{\"test\":\"test1\"}")) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = %+v, want {0, %d}", mp1, len("{\"test\":\"test1\"}"))
		}

		buf1, err := collected[0].Metadata()
		if err != nil {
			t.Fatal(err)
		}
		md1 := &appendable.IndexMeta{}
		if err := md1.UnmarshalBinary(buf1); err != nil {
			t.Fatal(err)
		}
		if md1.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md1.FieldType)
		}

		v2 := make([]byte, 8)
		binary.LittleEndian.PutUint64(v2, math.Float64bits(123))
		rv2, mp2, err := collected[1].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: v2})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[1].BPTree().Find(123) = nil, want non-nil")
		}

		if !bytes.Equal(rv2.Value, v2) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, v2)
		}

		if mp2.Offset != uint64(len("{\"test\":\"test1\"}\n")) || mp2.Length != uint32(len("{\"test\":123}")) {
			t.Errorf("got i.Indexes[1].BPTree().Find(123)= %+v, want {%d, %d}", mp2, len("{\"test\":\"test1\"}\n"), len("{\"test\":123}"))
		}

		md2 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md2); err != nil {
			t.Fatal(err)
		}
		if md2.FieldType != appendable.FieldTypeFloat64 {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeFloat64", md2.FieldType)
		}
	})

	t.Run("creates nested indices", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n{\"test2\":{\"a\":1,\"b\":\"2\"}}\n")); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(collected) != 4 {
			t.Errorf("got len(i.Indexes) = %d, want 4", len(collected))
		}

		md0 := &appendable.IndexMeta{}
		if err := collected[0].UnmarshalMetadata(md0); err != nil {
			t.Fatal(err)
		}

		md1 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md1); err != nil {
			t.Fatal(err)
		}

		md2 := &appendable.IndexMeta{}
		if err := collected[2].UnmarshalMetadata(md2); err != nil {
			t.Fatal(err)
		}

		md3 := &appendable.IndexMeta{}
		if err := collected[3].UnmarshalMetadata(md3); err != nil {
			t.Fatal(err)
		}

		if md0.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md0.FieldType)
		}

		if md1.FieldType != appendable.FieldTypeObject {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeObject", md1.FieldType)
		}

		if md2.FieldType != appendable.FieldTypeFloat64 {
			t.Errorf("got i.Indexes[2].FieldType = %#v, want FieldTypeFloat64", md2.FieldType)
		}

		if md3.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[3].FieldType = %#v, want FieldTypeString", md3.FieldType)
		}

		if md0.FieldName != "test" {
			t.Errorf("got i.Indexes[0].FieldName = %s, want \"test\"", md0.FieldName)
		}

		if md1.FieldName != "test2" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2\"", md1.FieldName)
		}

		if md2.FieldName != "test2.a" {
			t.Errorf("got i.Indexes[2].FieldName = %s, want \"a\"", md2.FieldName)
		}

		if md3.FieldName != "test2.b" {
			t.Errorf("got i.Indexes[3].FieldName = %s, want \"b\"", md3.FieldName)
		}
	})

	t.Run("creates second indices with same parent", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n{\"test\":{\"a\":1,\"b\":\"2\"}}\n")); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(collected) != 4 {
			t.Errorf("got len(i.Indexes) = %d, want 4", len(collected))
		}

		md0 := &appendable.IndexMeta{}
		if err := collected[0].UnmarshalMetadata(md0); err != nil {
			t.Fatal(err)
		}

		md1 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md1); err != nil {
			t.Fatal(err)
		}

		md2 := &appendable.IndexMeta{}
		if err := collected[2].UnmarshalMetadata(md2); err != nil {
			t.Fatal(err)
		}

		md3 := &appendable.IndexMeta{}
		if err := collected[3].UnmarshalMetadata(md3); err != nil {
			t.Fatal(err)
		}

		if md0.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md0.FieldType)
		}

		if md1.FieldType != appendable.FieldTypeObject {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeObject", md1.FieldType)
		}

		if md2.FieldType != appendable.FieldTypeFloat64 {
			t.Errorf("got i.Indexes[2].FieldType = %#v, want FieldTypeFloat64", md2.FieldType)
		}

		if md3.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[3].FieldType = %#v, want FieldTypeString", md3.FieldType)
		}

		if md0.FieldName != "test" {
			t.Errorf("got i.Indexes[0].FieldName = %s, want \"test\"", md0.FieldName)
		}

		if md1.FieldName != "test" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2\"", md1.FieldName)
		}

		if md2.FieldName != "test.a" {
			t.Errorf("got i.Indexes[2].FieldName = %s, want \"a\"", md2.FieldName)
		}

		if md3.FieldName != "test.b" {
			t.Errorf("got i.Indexes[3].FieldName = %s, want \"b\"", md3.FieldName)
		}
	})

	t.Run("creates index for arrays", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n{\"test2\":[[1,2,3],4]}\n")); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional data ranges but same number of indices
		if len(collected) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 2", len(collected))
		}

		md0 := &appendable.IndexMeta{}
		if err := collected[0].UnmarshalMetadata(md0); err != nil {
			t.Fatal(err)
		}

		md1 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md1); err != nil {
			t.Fatal(err)
		}

		if md0.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md0.FieldType)
		}

		if md1.FieldType != appendable.FieldTypeArray {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeObject", md1.FieldType)
		}

		if md0.FieldName != "test" {
			t.Errorf("got i.Indexes[0].FieldName = %s, want \"test\"", md0.FieldName)
		}

		if md1.FieldName != "test2" {
			t.Errorf("got i.Indexes[1].FieldName = %s, want \"test2\"", md1.FieldName)
		}
	})

	t.Run("existing index but nullable type", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte("{\"test\":\"test1\"}\n")); err != nil {
			t.Fatal(err)
		}

		r2 := []byte("{\"test\":\"test1\"}\n{\"test\":null}\n")
		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// check that the index file now has the additional index
		if len(collected) != 2 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}

		rv1, mp1, err := collected[0].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}
		if mp1 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != 0 || mp1.Length != uint32(len("{\"test\":\"test1\"}")) {
			t.Errorf("got i.Indexes[0].BPTree().Find(\"test1\") = %+v, want {0, %d}", mp1, len("{\"test\":\"test1\"}"))
		}

		buf1, err := collected[0].Metadata()
		if err != nil {
			t.Fatal(err)
		}
		md1 := &appendable.IndexMeta{}
		if err := md1.UnmarshalBinary(buf1); err != nil {
			t.Fatal(err)
		}
		if md1.FieldType != appendable.FieldTypeString {
			t.Errorf("got i.Indexes[0].FieldType = %#v, want FieldTypeString", md1.FieldType)
		}

		rv2, mp2, err := collected[1].BPTree(r2, JSONLHandler{}).Find(btree.ReferencedValue{})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (btree.MemoryPointer{}) {
			t.Errorf("got i.Indexes[1].BPTree().Find(null) = nil, want non-nil")
		}

		if len(rv2.Value) != 0 {
			t.Errorf("incorrect values, got %v, want %v", rv2.Value, "null")
		}

		if mp2.Offset != uint64(len("{\"test\":\"test1\"}\n")) || mp2.Length != uint32(len("{\"test\":null}")) {
			t.Errorf("got i.Indexes[1].BPTree().Find(\"test3\") = %+v, want {%d, %d}", mp2, len("{\"test\":\"test1\"}\n"), len("{\"test\":null}"))
		}

		buf2, err := collected[1].Metadata()
		if err != nil {
			t.Fatal(err)
		}
		md2 := &appendable.IndexMeta{}
		if err := md2.UnmarshalBinary(buf2); err != nil {
			t.Fatal(err)
		}
		if md2.FieldType != appendable.FieldTypeNull {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeNull", md2.FieldType)
		}
	})

	t.Run("recognize null fields", func(t *testing.T) {
		r1 := []byte("{\"nullheader\":null}\n")
		r2 := []byte("{\"nullheader\":null}\n{\"nullheader\":null}\n")

		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize(r1); err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collected) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}
		buf1, err := collected[0].Metadata()
		if err != nil {
			t.Fatal(err)
		}
		md1 := &appendable.IndexMeta{}

		if err := md1.UnmarshalBinary(buf1); err != nil {
			t.Fatal(err)
		}

		if md1.FieldName != "nullheader" || md1.FieldType != appendable.FieldTypeNull {
			t.Errorf("expected md1.FieldName nullheader, got: %v\nexpected field type to be null, got: %v", md1.FieldName, md1.FieldType)
		}
	})

	t.Run("correctly iterates through btree", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		i, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		r2 := []byte("{\"test\":1234}\n{\"test\":1234}\n")
		if err := i.Synchronize(r2); err != nil {
			t.Fatal(err)
		}

		indexes, err := i.Indexes()
		if err != nil {
			t.Fatal(err)
		}

		collected, err := indexes.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collected) != 1 {
			t.Errorf("got len(i.Indexes) = %d, want 1", len(collected))
		}

		v2 := make([]byte, 8)
		binary.LittleEndian.PutUint64(v2, math.Float64bits(1234))

		iter, err := collected[0].BPTree(r2, JSONLHandler{}).Iter(btree.ReferencedValue{Value: v2})
		if err != nil {
			t.Fatal(err)
		}

		idx := 0
		for ; iter.Next(); idx++ {
			if idx > 2 {
				t.Fatal("overflow")
			}

			k := iter.Key()
			if !bytes.Equal(k.Value, v2) {
				t.Fatal("keys are not equal")
			}
		}
	})
}
