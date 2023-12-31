package btree

import (
	"bytes"
	"reflect"
	"testing"
)

func TestNode(t *testing.T) {
	t.Run("encode", func(t *testing.T) {
		n := &Node{
			Size: 2,
			Keys: [8]DataPointer{
				{
					RecordOffset: 0,
					FieldOffset:  0,
					Length:       5,
				},
				{
					RecordOffset: 0,
					FieldOffset:  5,
					Length:       5,
				},
			},
			Children: [9]uint64{0, 1, 2},
			Leaf:     true,
		}
		buf := &bytes.Buffer{}
		if err := n.encode(buf); err != nil {
			t.Fatal(err)
		}
		if buf.Len() != 1+16*2+8*3 {
			t.Fatalf("expected buffer length to be 1+16*2+8*3, got %d", buf.Len())
		}
	})

	t.Run("decode", func(t *testing.T) {
		n := &Node{
			Size: 2,
			Keys: [8]DataPointer{
				{
					RecordOffset: 0,
					FieldOffset:  0,
					Length:       5,
				},
				{
					RecordOffset: 0,
					FieldOffset:  5,
					Length:       5,
				},
			},
			Children: [9]uint64{0, 1, 2},
			Leaf:     true,
		}
		buf := &bytes.Buffer{}
		if err := n.encode(buf); err != nil {
			t.Fatal(err)
		}
		m := &Node{}
		if err := m.decode(buf); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(n, m) {
			t.Fatalf("expected decoded node to be equal to original node")
		}
	})
}
