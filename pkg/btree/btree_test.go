package btree

import (
	"encoding/binary"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
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

func newTestMetaPage(t *testing.T, pf *pagefile.PageFile) *testMetaPage {
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
		tree := &BTree{PageFile: p, MetaPage: mp, Width: uint16(6)}
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
