package btree

import (
	"bytes"
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

func TestBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p)}
		// find a key that doesn't exist
		k, _, err := tree.Find(pointer.ReferencedValue{Value: []byte{1}})
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
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(6)}
		if err := tree.Insert(pointer.ReferencedValue{Value: []byte{1}}, hnsw.Point{1}); err != nil {
			t.Fatal(err)
		}
		k, v, err := tree.Find(pointer.ReferencedValue{Value: []byte{1}})

		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k.Value, []byte{1}) {
			t.Fatalf("expected to find key %v, got %v", []byte{1}, k.Value)
		}
		if v.Offset != 1 {
			t.Fatalf("expected value 1, got %d", v)
		}
	})

}
