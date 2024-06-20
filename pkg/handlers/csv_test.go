package handlers

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"math"
	"os"
	"testing"

	"github.com/kevmo314/appendable/pkg/pointer"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/bptree"
	"github.com/kevmo314/appendable/pkg/buftest"
)

func TestCSV(t *testing.T) {
	originalLogger := slog.Default()

	debugLevel := &slog.LevelVar{}
	debugLevel.Set(slog.LevelDebug)
	debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: debugLevel,
	}))

	slog.SetDefault(debugLogger)

	defer slog.SetDefault(originalLogger)

	t.Run("no schema changes", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()
		g := []byte("test\ntest1\n")

		var em []string

		i, err := appendable.NewIndexFile(f, CSVHandler{}, em)
		if err != nil {
			t.Fatal(err)
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
		r1 := []byte("test\ntest1\n")
		r2 := []byte("test\ntest1\ntest2\n")

		f := buftest.NewSeekableBuffer()

		var em []string

		i, err := appendable.NewIndexFile(f, CSVHandler{}, em)
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

		rv1, mp1, err := collected[0].BPTree(&bptree.BPTree{Data: r2, DataParser: CSVHandler{}}).Find(bptree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}

		if mp1 == (pointer.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].Btree().Find(test1) = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != uint64(len("test\n")) || mp1.Length != uint32(len("test1")) {
			t.Errorf("got i.Indexes[0].Btree().Find(\"test1\") = %+v, want {%d, %d}", mp1, len("test\n"), len("test1"))
		}

		rv2, mp2, err := collected[0].BPTree(&bptree.BPTree{Data: r2, DataParser: CSVHandler{}}).Find(bptree.ReferencedValue{Value: []byte("test2")})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (pointer.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].Btree().Find(\"test2\") = nil, want non-nil")
		}

		if !bytes.Equal(rv2.Value, []byte("test2")) {
			t.Errorf("incorrect values, got %v, want %v", rv2.Value, []byte("test2"))
		}

		if mp2.Offset != uint64(len("test\ntest1\n")) || mp2.Length != uint32(len("test2")) {
			t.Errorf("got i.Indexes[0].Btree().Find(\"test2\") = %+v, want {%d, %d}", mp2, len("test\ntest1\n"), len("test2"))
		}
	})
	t.Run("existing index but different type", func(t *testing.T) {

		s1 := "test\ntest1\n"
		s2 := "test\ntest1\n123\n"

		f := buftest.NewSeekableBuffer()

		var em []string
		i, err := appendable.NewIndexFile(f, CSVHandler{}, em)
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Synchronize([]byte(s1)); err != nil {
			t.Fatal(err)
		}

		r2 := []byte(s2)
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

		rv1, mp1, err := collected[0].BPTree(&bptree.BPTree{Data: r2, DataParser: CSVHandler{}}).Find(bptree.ReferencedValue{Value: []byte("test1")})
		if err != nil {
			t.Fatal(err)
		}
		if mp1 == (pointer.MemoryPointer{}) {
			t.Errorf("got i.Indexes[0].Btree().Find(\"test1\") = nil, want non-nil")
		}

		if !bytes.Equal(rv1.Value, []byte("test1")) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, []byte("test1"))
		}

		if mp1.Offset != uint64(len("test\n")) || mp1.Length != uint32(len("test1")) {
			t.Errorf("got i.Indexes[0].Btree().Find(\"test1\") = %+v, want {%d, %d}", mp1, len("test\n"), len("test1"))
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
		binary.BigEndian.PutUint64(v2, math.Float64bits(123))
		rv2, mp2, err := collected[1].BPTree(&bptree.BPTree{Data: r2, DataParser: CSVHandler{}}).Find(bptree.ReferencedValue{Value: v2})
		if err != nil {
			t.Fatal(err)
		}
		if mp2 == (pointer.MemoryPointer{}) {
			t.Errorf("got i.Indexes[1].Btree().Find(\"test3\") = nil, want non-nil")
		}
		if !bytes.Equal(rv2.Value, v2) {
			t.Errorf("incorrect values, got %v, want %v", rv1.Value, v2)
		}

		if mp2.Offset != uint64(len("test\ntest1\n")) || mp2.Length != uint32(len("123")) {
			t.Errorf("got i.Indexes[1].Btree().Find(\"test3\") = %+v, want {%d, %d}", mp2, len("test\ntest1\n"), len("123"))
		}

		md2 := &appendable.IndexMeta{}
		if err := collected[1].UnmarshalMetadata(md2); err != nil {
			t.Fatal(err)
		}
		if md2.FieldType != appendable.FieldTypeFloat64 {
			t.Errorf("got i.Indexes[1].FieldType = %#v, want FieldTypeFloat64", md2.FieldType)
		}
	})

	t.Run("recognize null fields", func(t *testing.T) {
		r1 := []byte("nullheader,header1\n,wef\n")
		r2 := []byte("nullheader,header1\n,wef\n,howdy\n")

		f := buftest.NewSeekableBuffer()

		var em []string

		i, err := appendable.NewIndexFile(f, CSVHandler{}, em)
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

		if len(collected) != 2 {
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

	t.Run("correctly iterates through bptree", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()
		var em []string
		i, err := appendable.NewIndexFile(f, CSVHandler{}, em)
		if err != nil {
			t.Fatal(err)
		}

		r2 := []byte("test\n1234\n1234\n")
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
		binary.BigEndian.PutUint64(v2, math.Float64bits(1234))

		iter, err := collected[0].BPTree(&bptree.BPTree{Data: r2, DataParser: JSONLHandler{}}).Iter(bptree.ReferencedValue{Value: v2})
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
