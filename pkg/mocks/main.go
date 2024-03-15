package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"io"
	"log"
	"math"
	"os"
)

func writeBufferToFile(buf *bytes.Buffer, filename string) error {
	if err := os.WriteFile(filename, buf.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}

func generateLeafNode() {
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

func generateInternalNode() {
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

func generate1023Btree() {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}

	mp, err := newTestMetaPage(p)

	if err != nil {
		log.Fatalf("%v", err)
	}
	tree := &btree.BPTree{PageFile: p, MetaPage: mp, Width: uint16(9)}
	count := 10

	for i := 0; i < count; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(23))

		if err := tree.Insert(btree.ReferencedValue{Value: buf, DataPointer: btree.MemoryPointer{Offset: uint64(i)}}, btree.MemoryPointer{Offset: uint64(i), Length: uint32(len(buf))}); err != nil {
			log.Fatal(err)
		}
	}

	b.WriteToDisk("bptree_1023.bin")
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

func generateUVariantTestCases() {
	var tests = []uint64{
		0,
		1,
		2,
		10,
		20,
		63,
		64,
		65,
		127,
		128,
		129,
		255,
		256,
		257,
		1<<63 - 1,
	}

	for _, x := range tests {
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, x)
		y, m := binary.Uvarint(buf[0:n])

		fmt.Printf("Test case - Value: %d, Encoded Bytes: %d\n", x, n)
		fmt.Printf("Decoded Value: %d, Bytes Read: %d\n", y, m)
	}
}

func main() {
	//generateUVariantTestCases()
	// generateFilledMetadata()
	// generateBasicBtree()
	// generateInternalNode()
	// generateLeafNode()
	//generateBtreeIterator()
	// generateFileMeta()
	// generateIndexMeta()

	generate1023Btree()
}
