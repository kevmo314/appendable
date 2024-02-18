package btree

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
)

type testMetaPage struct {
	root MemoryPointer
}

func (m *testMetaPage) SetRoot(mp MemoryPointer) error {
	m.root = mp
	return nil
}

func (m *testMetaPage) Root() (MemoryPointer, error) {
	return m.root, nil
}

func TestBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		// find a key that doesn't exist
		k, _, err := tree.Find(ReferencedValue{Value: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		if len(k.Value) != 0 {
			t.Fatal("expected not found")
		}
	})

	t.Run("insert creates a root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		if err := tree.Insert(ReferencedValue{Value: []byte("hello")}, MemoryPointer{Offset: 1}); err != nil {
			t.Fatal(err)
		}
		k, v, err := tree.Find(ReferencedValue{Value: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k.Value, []byte("hello")) {
			t.Fatal("expected to find key")
		}
		if v.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v)
		}
	})

	t.Run("insert into root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		if err := tree.Insert(ReferencedValue{Value: []byte("hello")}, MemoryPointer{Offset: 1}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("world")}, MemoryPointer{Offset: 2}); err != nil {
			t.Fatal(err)
		}
		k1, v1, err := tree.Find(ReferencedValue{Value: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k1.Value, []byte("hello")) {
			t.Fatal("expected to find key")
		}
		if v1.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v1)
		}
		k2, v2, err := tree.Find(ReferencedValue{Value: []byte("world")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k2.Value, []byte("world")) {
			t.Fatal("expected to find key")
		}
		if v2.Offset != 2 {
			t.Fatalf("expected value 2, got %d", v2)
		}
	})

	t.Run("split root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		if err := tree.Insert(ReferencedValue{Value: []byte("hello")}, MemoryPointer{Offset: 1}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("world")}, MemoryPointer{Offset: 2}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("moooo")}, MemoryPointer{Offset: 3}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("cooow")}, MemoryPointer{Offset: 4}); err != nil {
			t.Fatal(err)
		}

		if err := b.WriteToDisk("bptree_1.bin"); err != nil {
			t.Fatal(err)
		}

		k1, v1, err := tree.Find(ReferencedValue{Value: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k1.Value, []byte("hello")) {
			t.Fatal("expected to find key")
		}
		if v1.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v1)
		}
		k2, v2, err := tree.Find(ReferencedValue{Value: []byte("world")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k2.Value, []byte("world")) {
			t.Fatal("expected to find key")
		}
		if v2.Offset != 2 {
			t.Fatalf("expected value 2, got %d", v2)
		}
		k3, v3, err := tree.Find(ReferencedValue{Value: []byte("moooo")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k3.Value, []byte("moooo")) {
			t.Fatal("expected to find key")
		}
		if v3.Offset != 3 {
			t.Fatalf("expected value 3, got %d", v3)
		}
		k4, v4, err := tree.Find(ReferencedValue{Value: []byte("cooow")})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k4.Value, []byte("cooow")) {
			t.Fatal("expected to find key")
		}
		if v4.Offset != 4 {
			t.Fatalf("expected value 4, got %d", v4)
		}
	})

	t.Run("split intermediate", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		if err := tree.Insert(ReferencedValue{Value: []byte{0x05}}, MemoryPointer{Offset: 5}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x15}}, MemoryPointer{Offset: 15}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x25}}, MemoryPointer{Offset: 25}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x35}}, MemoryPointer{Offset: 35}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x45}}, MemoryPointer{Offset: 45}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBPTree_SequentialInsertionTest(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}
	tree := NewBPTree(p, &testMetaPage{})
	for i := 0; i < 256; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := tree.Insert(ReferencedValue{Value: buf}, MemoryPointer{Offset: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 256; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		k, v, err := tree.Find(ReferencedValue{Value: buf})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k.Value, buf) {
			t.Fatalf("expected to find key %d", i)
		}
		if v.Offset != uint64(i) {
			t.Fatalf("expected value %d, got %d", i, v)
		}
	}
}

type StubDataParser struct{}

func (s *StubDataParser) Parse(value []byte) []byte {
	return []byte{1, 2, 3, 4, 5, 6, 7, 8}
}

func TestBinarySearchReferencedValues(t *testing.T) {
	values := []ReferencedValue{
		{MemoryPointer{Offset: 0, Length: 10}, []byte{0}},
		{MemoryPointer{Offset: 10, Length: 20}, []byte{1}},
		{MemoryPointer{Offset: 20, Length: 30}, []byte{2}},
	}

	t.Run("find first key but zeroed memory pointer", func(t *testing.T) {
		key0 := ReferencedValue{MemoryPointer{}, []byte{0}}

		index0, found0 := slices.BinarySearchFunc(values, key0, CompareReferencedValues)

		if index0 != 0 {
			t.Fatalf("expected 0 got %v", index0)
		}

		// we expect false because we provide a memory pointer that's zeroed
		if found0 {
			t.Fatalf("expected false got %v", found0)
		}
	})

	t.Run("find key with correct memory pointer", func(t *testing.T) {

		key1 := ReferencedValue{MemoryPointer{Offset: 10, Length: 20}, []byte{1}}

		index1, found1 := slices.BinarySearchFunc(values, key1, CompareReferencedValues)

		if index1 != 1 {
			t.Fatalf("expected 1 got %v", index1)
		}

		if !found1 {
			t.Fatalf("expected true got %v", found1)
		}
	})

	t.Run("finds outof bounds index for non existent key", func(t *testing.T) {
		noKey := ReferencedValue{MemoryPointer{}, []byte{3}}

		undefIndex, undefFound := slices.BinarySearchFunc(values, noKey, CompareReferencedValues)
		if undefIndex != 3 {
			t.Fatalf("expected 3 got %v", undefIndex)
		}

		if undefFound {
			t.Fatalf("expected true got %v", undefFound)
		}
	})
}

func TestBPTree_RandomTests(t *testing.T) {
	t.Run("random insertion test", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		r := rand.New(rand.NewSource(12345))
		for i := 0; i < 65536; i++ {
			buf := make([]byte, 8)
			if _, err := r.Read(buf); err != nil {
				t.Fatal(err)
			}
			if err := tree.Insert(ReferencedValue{Value: buf}, MemoryPointer{Offset: uint64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		s := rand.New(rand.NewSource(12345))
		for i := 0; i < 65536; i++ {
			buf := make([]byte, 8)
			if _, err := s.Read(buf); err != nil {
				t.Fatal(err)
			}
			k, v, err := tree.Find(ReferencedValue{Value: buf})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(k.Value, buf) {
				t.Fatalf("expected to find key %d", i)
			}
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
	})

	t.Run("identical insertion test", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTreeWithData(p, &testMetaPage{}, make([]byte, 65536*4+8), &StubDataParser{})
		for i := 0; i < 65536*4; i++ {
			if err := tree.Insert(ReferencedValue{
				Value: []byte{1, 2, 3, 4, 5, 6, 7, 8},
				// DataPointer is used as a disambiguator.
				DataPointer: MemoryPointer{Offset: uint64(i), Length: 8},
			}, MemoryPointer{Offset: uint64(i)}); err != nil {
				t.Fatal(err)
			}
		}
	})

	// t.Run("bulk insert", func(t *testing.T) {
	// 	b := buftest.NewSeekableBuffer()
	// 	tree :=NewBPTree(b, 2)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	if err := tree.BulkInsert([]Entry{
	// 		{Key: []byte{0x05}, Value: 5},
	// 		{Key: []byte{0x15}, Value: 15},
	// 		{Key: []byte{0x25}, Value: 25},
	// 		{Key: []byte{0x35}, Value: 35},
	// 		{Key: []byte{0x45}, Value: 45},
	// 	}); err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	fmt.Printf(tree.String())
	// })
}
