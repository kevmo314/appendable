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
		k, _, err := tree.Find(pointer.ReferencedId{Value: hnsw.Id(0)})
		if err != nil {
			t.Fatal(err)
		}

		if k.Value != hnsw.Id(0) {
			t.Fatalf("expected id 0, got %d", k)
		}

	})

	t.Run("insert creates a root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(0), VectorDim: 1}
		if err := tree.Insert(pointer.ReferencedId{Value: 1}, hnsw.Point{1}); err != nil {
			t.Fatal(err)
		}
		k, _, err := tree.Find(pointer.ReferencedId{Value: 1})

		if err != nil {
			t.Fatal(err)
		}

		if k.Value != hnsw.Id(1) {
			t.Fatalf("expected id 1, got %d", k)
		}
	})

	t.Run("insert into root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(0), VectorDim: 1}
		if err := tree.Insert(pointer.ReferencedId{Value: 2}, hnsw.Point{2}); err != nil {
			t.Fatal(err)
		}

		if err := tree.Insert(pointer.ReferencedId{Value: 3}, hnsw.Point{3}); err != nil {
			t.Fatal(err)
		}

		k1, _, err := tree.Find(pointer.ReferencedId{Value: 2})
		if err != nil {
			t.Fatal(err)
		}

		if k1.Value != hnsw.Id(2) {
			t.Fatalf("expected id 2, got %d", k1)
		}

		k2, _, err := tree.Find(pointer.ReferencedId{Value: 3})
		if err != nil {
			t.Fatal(err)
		}

		if k2.Value != hnsw.Id(3) {
			t.Fatalf("expected id 3, got %d", k2)
		}
	})

}
