package btree

import (
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"math"
	"testing"
)

type testMetaPage struct {
	pf   *pagefile.PageFile
	root pointer.MemoryPointer
}

func (m *testMetaPage) SetRoot(mp pointer.MemoryPointer) error {
	m.root = mp
	return m.write()
}

func (m *testMetaPage) Root() (pointer.MemoryPointer, error) {
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

type Test interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

func newTestMetaPage(t Test, pf *pagefile.PageFile) *testMetaPage {
	meta := &testMetaPage{pf: pf}
	offset, err := pf.NewPage([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	// first page is garbage collection
	if offset != 4096 {
		t.Fatalf("expected offset 0, got %d", offset)
	}
	return meta
}

func TestBTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p)}
		// find a key that doesn't exist
		k, _, err := tree.Find(pointer.ReferencedId{Value: 1})
		if err != nil {
			t.Fatal(err)
		}
		if k.Value != 0 {
			t.Fatal("expected not found")
		}
	})

	t.Run("insert creates a root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), VectorDim: 2}
		if err := tree.Insert(pointer.ReferencedId{Value: 1}, hnsw.Point{2, 2}); err != nil {
			t.Fatal(err)
		}
		k, _, err := tree.Find(pointer.ReferencedId{Value: 1})
		if err != nil {
			t.Fatal(err)
		}
		if k.Value != 1 {
			t.Fatal("expected to find key")
		}
	})

	t.Run("insert into root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewBTree(newTestMetaPage(t, p), p, 2)
		if err := tree.Insert(pointer.ReferencedId{Value: 1}, hnsw.Point{1, 1}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 2}, hnsw.Point{2, 2}); err != nil {
			t.Fatal(err)
		}
		k1, _, err := tree.Find(pointer.ReferencedId{Value: 1})
		if err != nil {
			t.Fatal(err)
		}
		if k1.Value != 1 {
			t.Fatal("expected to find key")
		}

		k2, _, err := tree.Find(pointer.ReferencedId{Value: 2})
		if err != nil {
			t.Fatal(err)
		}
		if k2.Value != 2 {
			t.Fatal("expected to find key")
		}
	})

	t.Run("split root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		mp := newTestMetaPage(t, p)
		tree := &BTree{PageFile: p, MetaPage: mp, Width: uint16(6), VectorDim: 2}
		if err := tree.Insert(pointer.ReferencedId{Value: 1}, hnsw.Point{2, 2}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 2}, hnsw.Point{3, 3}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 3}, hnsw.Point{3, 4}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 4}, hnsw.Point{4, 4}); err != nil {
			t.Fatal(err)
		}

		k1, _, err := tree.Find(pointer.ReferencedId{Value: 1})
		if err != nil {
			t.Fatal(err)
		}
		if k1.Value != 1 {
			t.Fatal("expected to find key")
		}

		k2, _, err := tree.Find(pointer.ReferencedId{Value: 2})
		if err != nil {
			t.Fatal(err)
		}
		if k2.Value != 2 {
			t.Fatal("expected to find key")
		}
		k3, _, err := tree.Find(pointer.ReferencedId{Value: 3})
		if err != nil {
			t.Fatal(err)
		}
		if k3.Value != 3 {
			t.Fatal("expected to find key")
		}
		k4, _, err := tree.Find(pointer.ReferencedId{Value: 4})
		if err != nil {
			t.Fatal(err)
		}
		if k4.Value != 4 {
			t.Fatal("expected to find key")
		}
	})

	t.Run("split intermediate", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(2), VectorDim: 1}
		if err := tree.Insert(pointer.ReferencedId{Value: 1}, hnsw.Point{1}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 2}, hnsw.Point{2}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 3}, hnsw.Point{3}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 4}, hnsw.Point{4}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedId{Value: 5}, hnsw.Point{5}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBTree_SequentialInsertionTest(t *testing.T) {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		t.Fatal(err)
	}
	tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), VectorDim: 2}
	for i := 0; i < 256; i++ {
		if err := tree.Insert(pointer.ReferencedId{Value: hnsw.Id(i)}, hnsw.Point{float32(i), float32(i)}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 256; i++ {
		k, _, err := tree.Find(pointer.ReferencedId{Value: hnsw.Id(i)})
		if err != nil {
			t.Fatal(err)
		}
		if k.Value != hnsw.Id(i) {
			t.Fatalf("expected to find key %d", i)
		}
	}
}

func BenchmarkBTree(b *testing.B) {
	for i := 0; i <= 20; i++ {
		numRecords := int(math.Pow(2, float64(i)))

		b.Run(fmt.Sprintf("btree search %d_records", numRecords), func(b *testing.B) {
			buf := buftest.NewSeekableBuffer()
			p, err := pagefile.NewPageFile(buf)
			if err != nil {
				b.Fatal(err)
			}
			tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(b, p), VectorDim: 2}

			for rec := range numRecords {
				if err := tree.Insert(pointer.ReferencedId{Value: hnsw.Id(rec)}, hnsw.Point{float32(rec), float32(rec)}); err != nil {
					b.Fatalf("failed to insert record %d", rec)
				}
			}

			q := numRecords / 2
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				k, _, err := tree.Find(pointer.ReferencedId{Value: hnsw.Id(q)})
				if err != nil {
					b.Fatalf("failed to find record %d", q)
				}

				if k.Value != hnsw.Id(q) {
					b.Fatalf("expected to find key %d", q)
				}
			}
		})
	}
}
