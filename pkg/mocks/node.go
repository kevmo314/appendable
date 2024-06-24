package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/bptree"
	"github.com/kevmo314/appendable/pkg/pointer"
	"log"
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
	node1 := &bptree.BPTreeNode{
		LeafPointers: []pointer.MemoryPointer{
			{Offset: 0, Length: 3},
			{Offset: 3, Length: 3},
			{Offset: 6, Length: 3},
		},
		Keys: []pointer.ReferencedValue{
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
	node1 := &bptree.BPTreeNode{
		InternalPointers: []uint64{0, 1, 2, 3},
		Keys: []pointer.ReferencedValue{
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
