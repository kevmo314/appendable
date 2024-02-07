package btree

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBPTreeNode_ReadWriteLeaf(t *testing.T) {
	// Create a test BPTreeNode
	node1 := &BPTreeNode{
		LeafPointers: []MemoryPointer{
			{Offset: 0, Length: 1},
			{Offset: 1, Length: 2},
			{Offset: 2, Length: 3},
		},
		Keys: []ReferencedValue{
			{Value: []byte{0}},
			{Value: []byte{1, 2}},
			{Value: []byte{3, 4, 5}},
		},
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	node2 := &BPTreeNode{}
	if _, err := node2.ReadFrom(buf); err != nil {
		t.Fatal(err)
	}

	if !node2.leaf() {
		t.Fatal("expected leaf node")
	}

	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("expected %#v, got %#v", node1, node2)
	}
}

func TestBPTreeNode_ReadWriteIntermediate(t *testing.T) {
	// Create a test BPTreeNode
	node1 := &BPTreeNode{
		InternalPointers: []uint64{
			0, 1, 2, 3,
		},
		Keys: []ReferencedValue{
			{Value: []byte{0}},
			{Value: []byte{1, 2}},
			{Value: []byte{3, 4, 5}},
		},
	}

	buf := &bytes.Buffer{}
	if _, err := node1.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	node2 := &BPTreeNode{}
	if _, err := node2.ReadFrom(buf); err != nil {
		t.Fatal(err)
	}

	if node2.leaf() {
		t.Fatal("expected intermediate node")
	}

	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("expected %#v, got %#v", node1, node2)
	}
}
