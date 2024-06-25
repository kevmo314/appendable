package btree

import (
	"bytes"
	"encoding/binary"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"reflect"
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

func TestBTree_Insert(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p)}
		// find a key that doesn't exist
		k, _, _, err := tree.Find(pointer.ReferencedValue{Value: []byte("hello")})
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
		k, v, o, err := tree.Find(pointer.ReferencedValue{Value: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k.Value, []byte{1}) {
			t.Fatalf("expected to find key %v, got %v", []byte{1}, k.Value)
		}

		if !reflect.DeepEqual(v, hnsw.Point{1}) {
			t.Fatalf("expected to find point %v, got: %v", hnsw.Point{1}, v)
		}

		if o != 1 {
			t.Fatalf("expected value 1, got %d", o)
		}
	})

	t.Run("insert into root", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := &BTree{PageFile: p, MetaPage: newTestMetaPage(t, p), Width: uint16(6)}
		if err := tree.Insert(pointer.ReferencedValue{Value: []byte{1}}, hnsw.Point{1, 1}); err != nil {
			t.Fatal(err)
		}
		if err := tree.Insert(pointer.ReferencedValue{Value: []byte{2}}, hnsw.Point{2, 2}); err != nil {
			t.Fatal(err)
		}
		k1, v1, o1, err := tree.Find(pointer.ReferencedValue{Value: []byte{1}})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k1.Value, []byte{1}) {
			t.Fatalf("expected to find key %v, got %v", []byte{1}, k1.Value)
		}

		if !reflect.DeepEqual(hnsw.Point{1, 1}, v1) {
			t.Fatalf("expected to find point %v, got: %v", hnsw.Point{1, 1}, v1)
		}

		if o1 != 1 {
			t.Fatalf("expected value 1, got %d", o1)
		}
		k2, v2, o2, err := tree.Find(pointer.ReferencedValue{Value: []byte{2}})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(k2.Value, []byte{2}) {
			t.Fatalf("expected to find key %v, got %v", []byte{2}, k2.Value)

		}

		if !reflect.DeepEqual(hnsw.Point{2, 2}, v2) {
			t.Fatalf("expected to find point %v, got: %v", hnsw.Point{2, 2}, v2)
		}

		if o2 != 2 {
			t.Fatalf("expected value 2, got %d", o2)
		}
	})
}
