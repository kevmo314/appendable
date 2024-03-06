package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/buftest"
)

type testMetaPage struct {
	pf   *btree.PageFile
	root btree.MemoryPointer
}

func (m *testMetaPage) GetWidth() uint16 {
	return ^uint16(0)
}

func (m *testMetaPage) SetRoot(mp btree.MemoryPointer) error {
	m.root = mp
	return m.write()
}

func (m *testMetaPage) Root() (btree.MemoryPointer, error) {
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

func newTestMetaPage(pf *btree.PageFile) (*testMetaPage, error) {
	meta := &testMetaPage{pf: pf}
	offset, err := pf.NewPage([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	// first page is garbage collection
	if offset != 4096 {
		return nil, fmt.Errorf("expected offset 0, got %d", offset)
	}
	return meta, nil
}

func generateBasicBtree() {
	b := buftest.NewSeekableBuffer()
	p, err := btree.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}
	mp, err := newTestMetaPage(p)

	if err != nil {
		log.Fatalf("%v", err)
	}

	tree := btree.NewBPTree(p, mp)
	if err := tree.Insert(btree.ReferencedValue{Value: []byte("hello")}, btree.MemoryPointer{Offset: 1, Length: 5}); err != nil {
		log.Fatalf("%v", err)
	}
	if err := tree.Insert(btree.ReferencedValue{Value: []byte("world")}, btree.MemoryPointer{Offset: 2, Length: 5}); err != nil {
		log.Fatalf("%v", err)
	}
	if err := tree.Insert(btree.ReferencedValue{Value: []byte("moooo")}, btree.MemoryPointer{Offset: 3, Length: 5}); err != nil {
		log.Fatalf("%v", err)
	}
	if err := tree.Insert(btree.ReferencedValue{Value: []byte("cooow")}, btree.MemoryPointer{Offset: 4, Length: 5}); err != nil {
		log.Fatalf("%v", err)
	}

	if err := b.WriteToDisk("bptree_1.bin"); err != nil {
		log.Fatalf("%v", err)
	}
}

type StubDataParser struct{}

func (s *StubDataParser) Parse(value []byte) []byte {
	return []byte{1, 2, 3, 4, 5, 6, 7, 8}
}

func generateBtreeIterator() {

	b := buftest.NewSeekableBuffer()
	p, err := btree.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}

	mp, err := newTestMetaPage(p)

	if err != nil {
		log.Fatalf("%v", err)
	}
	tree := btree.NewBPTreeWithData(p, mp, make([]byte, 16384*4+8), &StubDataParser{})
	for i := 0; i < 16384*4; i++ {
		if err := tree.Insert(btree.ReferencedValue{
			Value: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			// DataPointer is used as a disambiguator.
			DataPointer: btree.MemoryPointer{Offset: uint64(i), Length: 8},
		}, btree.MemoryPointer{Offset: uint64(i)}); err != nil {
			log.Fatalf("%v", err)
		}
	}

	b.WriteToDisk("btree_iterator.bin")
}

func generateFilledMetadata() {
	b := buftest.NewSeekableBuffer()
	p, err := btree.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}
	tree, err := btree.NewMultiBPTree(p, 0)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := tree.Reset(); err != nil {
		log.Fatalf("%v", err)
	}
	if err := tree.SetMetadata([]byte("hello")); err != nil {
		log.Fatalf("%v", err)
	}

	b.WriteToDisk("filled_metadata.bin")
}

func main() {

	generateFilledMetadata()
	//generateBasicBtree()
	//generateBtreeIterator()

}
