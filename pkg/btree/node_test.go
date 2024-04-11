package btree

import (
	"bytes"
	"github.com/kevmo314/appendable/pkg/pointer"
	"reflect"
	"testing"
)

func TestBTreeNode_ReadWriteLeaf(t *testing.T) {
	// Create a test BTreeNode
	node1 := &BTreeNode{
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

	node2 := &BTreeNode{Width: uint16(4)}
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

func TestBTreeNode_ReadWriteIntermediate(t *testing.T) {
	// Create a test BTreeNode
	node1 := &BTreeNode{
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

	node2 := &BTreeNode{Width: uint16(3)}
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

func TestBTreeNode_CompareReferencedValues(t *testing.T) {
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
