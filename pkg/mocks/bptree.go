package main

import (
	"encoding/binary"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"log"
	"math"
)

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
