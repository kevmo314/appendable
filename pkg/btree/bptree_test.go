package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

type testMetaPage struct {
	pf   *pagefile.PageFile
	root MemoryPointer
}

func (m *testMetaPage) SetRoot(mp MemoryPointer) error {
	m.root = mp
	return m.write()
}

func (m *testMetaPage) Root() (MemoryPointer, error) {
	return m.root, nil
}

func (m *testMetaPage) write() error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, m.root.Offset)
	if _, err := m.pf.Seek(4096, io.SeekStart); err != nil {
		return err
	}
	if _, err := m.pf.Write(buf); err != nil {
		return err
	}
	return nil
}

func newTestMetaPage(t *testing.T, pf *pagefile.PageFile) *testMetaPage {
	meta := &testMetaPage{pf: pf}
	offset, err := pf.NewPage([]byte{0, 0, 0, 0, 0, 0, 0, 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// first page is garbage collection
	if offset != 4096 {
		t.Fatalf("expected offset 0, got %d", offset)
	}
	return meta
}

func TestBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p)}
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
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(6)}
		if err := tree.Insert(ReferencedValue{Value: []byte("hello")}, MemoryPointer{Offset: 1, Length: 5}); err != nil {
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
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(6)}
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
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		mp := newTestMetaPage(t, p)
		tree := &BPTree{PageFile: p, MetaPage: mp, Width: uint16(6)}
		if err := tree.Insert(ReferencedValue{Value: []byte("hello")}, MemoryPointer{Offset: 1, Length: 5}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("world")}, MemoryPointer{Offset: 2, Length: 5}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("moooo")}, MemoryPointer{Offset: 3, Length: 5}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte("cooow")}, MemoryPointer{Offset: 4, Length: 5}); err != nil {
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
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(2)}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x05}}, MemoryPointer{Offset: 5}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x10}}, MemoryPointer{Offset: 10}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x15}}, MemoryPointer{Offset: 15}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x20}}, MemoryPointer{Offset: 20}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(ReferencedValue{Value: []byte{0x25}}, MemoryPointer{Offset: 25}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBPTree_SequentialInsertionTest(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}
	tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(9)}
	for i := 0; i < 256; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := tree.Insert(ReferencedValue{Value: buf}, MemoryPointer{Offset: uint64(i), Length: uint32(len(buf))}); err != nil {
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

func TestBPTree_RandomTests(t *testing.T) {
	t.Run("random insertion test", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(9)}
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
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Data: make([]byte, 65536*4+8), DataParser: &StubDataParser{}}
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

func TestBPTree_Iteration(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}

	metaPage := newTestMetaPage(t, p)
	tree := &BPTree{PageFile: p, MetaPage: metaPage, Data: make([]byte, 16384*4+8), DataParser: &StubDataParser{}}
	for i := 0; i < 16384*4; i++ {
		if err := tree.Insert(ReferencedValue{
			Value: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			// DataPointer is used as a disambiguator.
			DataPointer: MemoryPointer{Offset: uint64(i), Length: 8},
		}, MemoryPointer{Offset: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("forward iteration", func(t *testing.T) {
		iter, err := tree.Iter(ReferencedValue{Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}})
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for ; iter.Next(); i++ {
			if i > 16384*4 {
				t.Fatalf("expected to find %d keys", 16384*4)
			}
			k := iter.Key()
			if !bytes.Equal(k.Value, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
				t.Fatalf("expected to find key %d", i)
			}
			v := iter.Pointer()
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
		if i != 16384*4 {
			t.Fatalf("expected to find %d keys, got %d", 16384*4, i)
		}
	})

	t.Run("reverse iteration", func(t *testing.T) {
		iter, err := tree.Iter(ReferencedValue{Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}, DataPointer: MemoryPointer{Offset: math.MaxUint64}})
		if err != nil {
			t.Fatal(err)
		}
		i := 16384*4 - 1
		for ; iter.Prev(); i-- {
			if i < 0 {
				t.Fatalf("expected to find %d keys", 16384*4)
			}
			k := iter.Key()
			if !bytes.Equal(k.Value, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
				t.Fatalf("expected to find key %d", i)
			}
			v := iter.Pointer()
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
		if i != -1 {
			t.Fatalf("expected to find %d keys, got %d", 16384*4, i)
		}
	})
}

func TestBPTree_Iteration_SinglePage(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}
	metaPage := newTestMetaPage(t, p)
	tree := &BPTree{PageFile: p, MetaPage: metaPage, Data: make([]byte, 64+8), DataParser: &StubDataParser{}}
	for i := 0; i < 64; i++ {
		if err := tree.Insert(ReferencedValue{
			Value: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			// DataPointer is used as a disambiguator.
			DataPointer: MemoryPointer{Offset: uint64(i), Length: 8},
		}, MemoryPointer{Offset: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("forward iteration", func(t *testing.T) {
		iter, err := tree.Iter(ReferencedValue{Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}})
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for ; iter.Next(); i++ {
			if i > 64 {
				t.Fatalf("expected to find %d keys", 64)
			}
			k := iter.Key()
			if !bytes.Equal(k.Value, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
				t.Fatalf("expected to find key %d", i)
			}
			v := iter.Pointer()
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
		if i != 64 {
			t.Fatalf("expected to find %d keys, got %d", 64, i)
		}
	})

	t.Run("reverse iteration", func(t *testing.T) {
		iter, err := tree.Iter(ReferencedValue{Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}, DataPointer: MemoryPointer{Offset: math.MaxUint64}})
		if err != nil {
			t.Fatal(err)
		}
		i := 64 - 1
		for ; iter.Prev(); i-- {
			if i < 0 {
				t.Fatalf("expected to find %d keys", 64)
			}
			k := iter.Key()
			if !bytes.Equal(k.Value, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
				t.Fatalf("expected to find key %d", i)
			}
			v := iter.Pointer()
			if v.Offset != uint64(i) {
				t.Fatalf("expected value %d, got %d", i, v)
			}
		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
		if i != -1 {
			t.Fatalf("expected to find %d keys, got %d", 64, i)
		}
	})
}

func TestBPTree_Iteration_FirstLast(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}
	tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(9)}
	start := 10.0
	increments := []float64{0.01, 0.05, 0.3}
	currentIncrementIndex := 0

	for i := start; i < 256; i += increments[currentIncrementIndex] {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(i))

		if err := tree.Insert(ReferencedValue{Value: buf}, MemoryPointer{Offset: uint64(i * 100), Length: uint32(len(buf))}); err != nil {
			t.Fatal(err)
		}

		if int(i*100)%10 == 0 && currentIncrementIndex < len(increments)-1 {
			currentIncrementIndex++
		}
	}

	t.Run("find first and iter", func(t *testing.T) {
		first, err := tree.first()
		if err != nil {
			t.Fatal(err)
		}
		firstBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(firstBuf, math.Float64bits(10))

		if !bytes.Equal(first.Value, firstBuf) {
			t.Fatal("expected 10 as first reference value")
		}

		iter, err := tree.Iter(first)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		var p *float64 = nil
		for ; iter.Next(); i++ {
			k := iter.Key()
			var c float64
			reader := bytes.NewReader(k.Value)
			err := binary.Read(reader, binary.BigEndian, &c)
			if err != nil {
				fmt.Println("binary.Read failed:", err)
				return
			}

			if p != nil && *p > c {
				t.Errorf("expected a non-decreasing traversal but got prev: %v, curr: %v", *p, c)
				return
			}
			p = &c
		}

	})

	t.Run("find last and iter", func(t *testing.T) {
		last, err := tree.last()
		if err != nil {
			t.Fatal(err)
		}

		iter, err := tree.Iter(last)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		var r *float64 = nil
		for ; iter.Prev(); i++ {
			k := iter.Key()
			var l float64
			reader := bytes.NewReader(k.Value)
			err := binary.Read(reader, binary.BigEndian, &l)
			if err != nil {
				fmt.Println("binary.Read failed:", err)
				return
			}

			if r != nil && !(l <= *r) {
				t.Errorf("expected a non-increasing traversal but got curr: %v, prev: %v", l, *r)
				return
			}
			r = &l
		}

	})
}

func TestBPTree_IncorrectWidth(t *testing.T) {

	t.Run("float tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		floatTree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(9)}

		if err := floatTree.Insert(ReferencedValue{Value: []byte{1}, DataPointer: MemoryPointer{Offset: uint64(0)}}, MemoryPointer{Offset: uint64(0), Length: uint32(39)}); err == nil {
			t.Fatalf("should error %v", err)
		}
	})

	t.Run("nil tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		nilTree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(1)}

		if err := nilTree.Insert(ReferencedValue{Value: []byte{1}, DataPointer: MemoryPointer{Offset: uint64(0)}}, MemoryPointer{Offset: uint64(0), Length: uint32(39)}); err == nil {
			t.Fatalf("should error %v", err)
		}
	})
}

func TestBPTree_Iteration_Overcount(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}

	tree := &BPTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(9)}
	count := 10

	for i := 0; i < count; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(23))

		if err := tree.Insert(ReferencedValue{Value: buf, DataPointer: MemoryPointer{Offset: uint64(i)}}, MemoryPointer{Offset: uint64(i), Length: uint32(len(buf))}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("finds exactly 10 occurrences of 23", func(t *testing.T) {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(23))

		valueRef := ReferencedValue{
			Value: buf,
		}

		iter, err := tree.Iter(valueRef)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for ; iter.Next(); i++ {
			k := iter.Key()
			reader := bytes.NewReader(k.Value)
			var c float64
			err := binary.Read(reader, binary.BigEndian, &c)
			if err != nil {
				t.Fatal(err)
			}

			if c != float64(23) {
				t.Errorf("expected c == 23, got: %v", c)
			}
		}

		if i != 10 {
			t.Errorf("should've iterated 10 times, got %v", i)
		}

	})

}
