package btree

import (
	"encoding/binary"
	"math/rand"
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
		_, found, err := tree.Find([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if found {
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
		v, found, err := tree.Find([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if !found {
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
		v1, f1, err := tree.Find([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if !f1 {
			t.Fatal("expected to find key")
		}
		if v1.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v1)
		}
		v2, f2, err := tree.Find([]byte("world"))
		if err != nil {
			t.Fatal(err)
		}
		if !f2 {
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
		v1, f1, err := tree.Find([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if !f1 {
			t.Fatal("expected to find key")
		}
		if v1.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v1)
		}
		v2, f2, err := tree.Find([]byte("world"))
		if err != nil {
			t.Fatal(err)
		}
		if !f2 {
			t.Fatal("expected to find key")
		}
		if v2.Offset != 2 {
			t.Fatalf("expected value 2, got %d", v2)
		}
		v3, f3, err := tree.Find([]byte("moooo"))
		if err != nil {
			t.Fatal(err)
		}
		if !f3 {
			t.Fatal("expected to find key")
		}
		if v3.Offset != 3 {
			t.Fatalf("expected value 3, got %d", v3)
		}
		v4, f4, err := tree.Find([]byte("cooow"))
		if err != nil {
			t.Fatal(err)
		}
		if !f4 {
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

	t.Run("insertion test", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBPTree(p, &testMetaPage{})
		for i := 0; i < 16384; i++ {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(i))
			if err := tree.Insert(ReferencedValue{Value: buf}, MemoryPointer{Offset: uint64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 16384; i++ {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(i))
			v, found, err := tree.Find(buf)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("expected to find key %d", i)
			}
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
	})

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
			v, found, err := tree.Find(buf)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
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
		tree := NewBPTree(p, &testMetaPage{})
		for i := 0; i < 65536*4; i++ {
			if err := tree.Insert(ReferencedValue{Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}}, MemoryPointer{Offset: uint64(i)}); err != nil {
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
