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
		if _, err := n.WriteTo(buf); err != nil {
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
		if _, err := n.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		m := &Node{}
		if _, err := m.ReadFrom(buf); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(n, m) {
			t.Fatalf("expected decoded node to be equal to original node")
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
