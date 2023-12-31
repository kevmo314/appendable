package btree

import (
	"bytes"
	"reflect"
	"testing"
)

func TestNode(t *testing.T) {
	t.Run("encode leaf", func(t *testing.T) {
		n := &Node{
			Keys: []DataPointer{
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
			Leaf: true,
		}
		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		if buf.Len() != 1+16*2 {
			t.Fatalf("expected buffer length to be 1+16*2+8*3, got %d", buf.Len())
		}
	})

	t.Run("encode leaf ignores children", func(t *testing.T) {
		n := &Node{
			Keys: []DataPointer{
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
			Leaf:     true,
			Children: []uint64{1, 2, 3},
		}
		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		if buf.Len() != 1+16*2 {
			t.Fatalf("expected buffer length to be 1+16*2+8*3, got %d", buf.Len())
		}
	})

	t.Run("encode non-leaf", func(t *testing.T) {
		n := &Node{
			Keys: []DataPointer{
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
			Leaf:     false,
			Children: []uint64{1, 2, 3},
		}
		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		if buf.Len() != 1+16*2+8*3 {
			t.Fatalf("expected buffer length to be 1+16*2+8*3, got %d", buf.Len())
		}
	})

	t.Run("decode leaf", func(t *testing.T) {
		n := &Node{
			Keys: []DataPointer{
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
			Leaf: true,
		}
		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		m := &Node{}
		if _, err := m.ReadFrom(buf); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(n, m) {
			t.Fatalf("expected decoded node to be equal to original node, got %#v want %#v", m, n)
		}
	})

	t.Run("decode non-leaf", func(t *testing.T) {
		n := &Node{
			Keys: []DataPointer{
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
			Leaf:     false,
			Children: []uint64{1, 2, 3},
		}
		buf := &bytes.Buffer{}
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		m := &Node{}
		if _, err := m.ReadFrom(buf); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(n, m) {
			t.Fatalf("expected decoded node to be equal to original node, got %#v want %#v", m, n)
		}
	})
}

func TestDataPointer(t *testing.T) {
	buf := newSeekableBuffer()
	if _, err := buf.Write([]byte("moocowslmao")); err != nil {
		t.Fatal(err)
	}
	p := DataPointer{
		RecordOffset: 1,
		FieldOffset:  2,
		Length:       5,
	}
	b, err := p.Value(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte("cowsl")) {
		t.Fatalf("expected value to be ocows, got %s", string(b))
	}
}
