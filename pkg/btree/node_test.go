package btree

import (
	"bytes"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pointer"
	"reflect"
	"testing"
)

func TestBTreeNode_Size(t *testing.T) {
	t.Run("node size", func(t *testing.T) {
		n := &BTreeNode{ // 4
			Ids:       []pointer.ReferencedId{{Value: 1}, {Value: 2}, {Value: 3}}, // 3 * (3)
			Vectors:   []hnsw.Point{{1, 1}, {2, 2}, {3, 3}},                       // 6 * 4 == 3 * 2 * 4 // 24
			Offsets:   make([]uint64, 0),
			VectorDim: 2, // 1
		}

		if n.Size() != 38 {
			t.Fatalf("wrong size: %d", n.Size())
		}
	})
}

func TestBTreeNode_MarshalBinary(t *testing.T) {
	t.Run("leaf node", func(t *testing.T) {
		n := &BTreeNode{
			Ids: []pointer.ReferencedId{
				{Value: 1},
				{Value: 2},
				{Value: 3},
			},
			Vectors:   []hnsw.Point{{0, 0}, {0, 0}, {0, 0}},
			Offsets:   make([]uint64, 0),
			VectorDim: 2,
		}

		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}

		m := &BTreeNode{}
		if err := m.UnmarshalBinary(buf.Bytes()); err != nil {
			t.Fatal(err)
		}

		if !m.Leaf() {
			t.Fatalf("expected leaf node, but got %v offsets", len(m.Offsets))
		}

		if !reflect.DeepEqual(n, m) {
			t.Fatalf("encoded\n%#v\ndecoded\n%#v", n, m)
		}
	})

	t.Run("intermediate node", func(t *testing.T) {
		n := &BTreeNode{
			Ids: []pointer.ReferencedId{
				{Value: 1},
				{Value: 2},
				{Value: 3},
			},
			Vectors:   []hnsw.Point{{0, 0}, {0, 0}, {0, 0}},
			Offsets:   []uint64{0, 4096, 8192, 6969},
			VectorDim: 2,
		}

		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}

		m := &BTreeNode{}
		if err := m.UnmarshalBinary(buf.Bytes()); err != nil {
			t.Fatal(err)
		}

		if m.Leaf() {
			t.Fatal("expected intermediate node")
		}

		if !reflect.DeepEqual(n, m) {
			t.Fatalf("encoded\n%#v\ndecoded\n%#v", n, m)
		}
	})
}
