package btree

import (
	"bytes"
	"encoding/binary"
	"github.com/kevmo314/appendable/pkg/pointer"
	"reflect"
	"testing"
)

func TestBPTreeNode_ReadWriteLeaf(t *testing.T) {
	// Create a test BPTreeNode
	node1 := &BPTreeNode{
		LeafPointers: []pointer.MemoryPointer{
			{Offset: 0, Length: 3},
			{Offset: 3, Length: 3},
			{Offset: 6, Length: 3},
		},
		Keys: []ReferencedValue{
			{Value: []byte{0, 1, 2}},
			{Value: []byte{1, 2, 3}},
			{Value: []byte{3, 4, 5}},
		},
		Width: uint16(4),
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	node2 := &BPTreeNode{Width: uint16(4)}
	if err := node2.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	if !node2.Leaf() {
		t.Fatal("expected leaf node")
	}

	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("expected %#v\ngot %#v", node1, node2)
	}
}

func TestBPTreeNode_ReadWriteIntermediate(t *testing.T) {
	// Create a test BPTreeNode
	node1 := &BPTreeNode{
		InternalPointers: []uint64{0, 1, 2, 3},
		Keys: []ReferencedValue{
			{Value: []byte{0, 1}},
			{Value: []byte{1, 2}},
			{Value: []byte{3, 4}},
		},
		Width: uint16(3),
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	node2 := &BPTreeNode{Width: uint16(3)}
	if err := node2.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	if node2.Leaf() {
		t.Fatal("expected intermediate node")
	}

	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("expected %#v, got %#v", node1, node2)
	}
}

func TestBPTreeNode_CompareReferencedValues(t *testing.T) {
	rv := []ReferencedValue{
		{
			Value: []byte{0},
		},
		{
			Value:       []byte{1},
			DataPointer: pointer.MemoryPointer{Offset: 0},
		}, {
			Value:       []byte{1},
			DataPointer: pointer.MemoryPointer{Offset: 1},
		}, {
			Value:       []byte{1},
			DataPointer: pointer.MemoryPointer{Offset: 1, Length: 1},
		},
	}
	for i := 0; i < len(rv); i++ {
		for j := 0; j < len(rv); j++ {
			cmp := CompareReferencedValues(rv[i], rv[j])
			if i < j && cmp >= 0 {
				t.Fatalf("expected %d < %d", i, j)
			}
			if i > j && cmp <= 0 {
				t.Fatalf("expected %d > %d", i, j)
			}
			if i == j && cmp != 0 {
				t.Fatalf("expected %d == %d", i, j)
			}
		}
	}
}

func TestMarshalDuplicate(t *testing.T) {
	node1 := &BPTreeNode{
		InternalPointers: []uint64{0, 1, 2, 3},
		Keys: []ReferencedValue{
			{Value: []byte{0, 1}},
			{Value: []byte{1, 2}},
			{Value: []byte{3, 4}},
			{Value: []byte{3, 4}},
		},
		Width: uint16(3),
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	node2 := &BPTreeNode{Width: uint16(3)}
	if err := node2.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("\ne: %#v\ng: %#v\n", node1, node2)
	}
}

func TestSizeVariant(t *testing.T) {

	x := len(binary.AppendUvarint([]byte{}, uint64(123)))
	y := SizeVariant(uint64(123))

	if x != y {
		t.Fatalf("expected x == y, got %v == %v", x, y)
	}
}
