package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

func writeBufferToFile(buf *bytes.Buffer, filename string) error {
	if err := os.WriteFile(filename, buf.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}

func generateBPLeafNode() {
	// Create a test BPTreeNode
	node1 := &btree.BPTreeNode{
		LeafPointers: []btree.MemoryPointer{
			{Offset: 0, Length: 3},
			{Offset: 3, Length: 3},
			{Offset: 6, Length: 3},
		},
		Keys: []btree.ReferencedValue{
			{Value: []byte{0, 1, 2}},
			{Value: []byte{1, 2, 3}},
			{Value: []byte{3, 4, 5}},
		},
		Width: uint16(4),
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		log.Fatal(err)
	}

	writeBufferToFile(buf, "leafnode.bin")
}

func generateBPInternalNode() {
	// Create a test BPTreeNode
	node1 := &btree.BPTreeNode{
		InternalPointers: []uint64{0, 1, 2, 3},
		Keys: []btree.ReferencedValue{
			{Value: []byte{0, 1}},
			{Value: []byte{1, 2}},
			{Value: []byte{3, 4}},
		},
		Width: uint16(3),
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		log.Fatal(err)
	}

	writeBufferToFile(buf, "internalnode.bin")

}

type testMetaPage struct {
	pf   *pagefile.PageFile
	root btree.MemoryPointer
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

func newTestMetaPage(pf *pagefile.PageFile) (*testMetaPage, error) {
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
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}
	mp, err := newTestMetaPage(p)

	if err != nil {
		log.Fatalf("%v", err)
	}

	tree := &btree.BPTree{PageFile: p, MetaPage: mp, Width: uint16(6)}
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
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}

	mp, err := newTestMetaPage(p)

	if err != nil {
		log.Fatalf("%v", err)
	}
	tree := &btree.BPTree{PageFile: p, MetaPage: mp, Data: make([]byte, 16384*4+8), DataParser: &StubDataParser{}, Width: uint16(0)}
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
	p, err := pagefile.NewPageFile(b)
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

func writeByteToFile(data []byte, filename string) error {
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	return nil
}

func generateFileMeta() {
	fm := appendable.FileMeta{}
	fm.Format = 1
	fm.Version = 1
	fm.ReadOffset = 4096

	b, err := fm.MarshalBinary()
	if err != nil {
		log.Fatalf("failed to write file meta to disk")
	}

	if err := writeByteToFile(b, "filemeta.bin"); err != nil {
		log.Fatalf("failed to write bytes to disk")
	}
}

func generateIndexMeta() {
	im := appendable.IndexMeta{}
	im.FieldName = "howdydo"
	im.FieldType = appendable.FieldTypeBoolean
	im.Width = appendable.DetermineType(appendable.FieldTypeBoolean)

	b, err := im.MarshalBinary()
	if err != nil {
		log.Fatal("failed to write index meta to disk")
	}

	if err := writeByteToFile(b, "indexmeta.bin"); err != nil {
		log.Fatalf("failed to write bytes to disk")
	}
}

func main() {

	// generateFilledMetadata()
	// generateBasicBtree()
	//generateBtreeIterator()
	// generateFileMeta()
	// generateIndexMeta()

	generateBPLeafNode()
	generateBPInternalNode()
}
