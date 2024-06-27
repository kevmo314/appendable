package btree

import (
	"encoding/binary"
	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
)

type BTreeNode struct {
	Keys    []pointer.ReferencedId
	Vectors []hnsw.Point

	Offsets []uint64

	// Width should be 0 for varint
	Width uint16
}

func (n *BTreeNode) Size() int64 {
	size := 4

	for _, k := range n.Keys {
		size += encoding.SizeVarint(k.DataPointer.Offset)
		size += encoding.SizeVarint(uint64(k.Id))
	}

	for _, n := range n.Offsets {
		size += encoding.SizeVarint(n)
	}

	return int64(size)
}

func (n *BTreeNode) MarshalBinary() ([]byte, error) {
	size := int32(len(n.Keys))

	if size == 0 {
		panic("writing empty node")
	}

	buf := make([]byte, n.Size())

	if n.Leaf() {
		binary.LittleEndian.PutUint32(buf[:4], uint32(-size))
	} else {
		binary.LittleEndian.PutUint32(buf[:4], uint32(size))
	}

	ct := 4
	for _, k := range n.Keys {
		on := binary.PutUvarint(buf[ct:], k.DataPointer.Offset)
		vn := binary.PutUvarint(buf[ct+on:], uint64(k.Id))
		ct += on + vn

	}

	for _, o := range n.Offsets {
		on := binary.PutUvarint(buf[ct:], o)
		ct += on
	}

	if ct != int(n.Size()) {
		panic("size mismatch")
	}

	return buf, nil
}

func (n *BTreeNode) UnmarshalBinary(buf []byte) error {
	size := int32(binary.LittleEndian.Uint32(buf[:4]))
	leaf := size < 0

	if leaf {
		n.Offsets = make([]uint64, (-size)+1)
		n.Keys = make([]pointer.ReferencedId, -size)
		n.Vectors = make([]hnsw.Point, -size)
	} else {
		n.Keys = make([]pointer.ReferencedId, size)
		n.Vectors = make([]hnsw.Point, size)
	}

	if size == 0 {
		panic("empty node")
	}

	m := 4
	for i := range n.Keys {
		o, on := binary.Uvarint(buf[m:])
		v, vn := binary.Uvarint(buf[m+on:])

		n.Keys[i].Id = hnsw.Id(v)
		n.Keys[i].DataPointer.Offset = o

		m += on + vn
	}

	for i := range n.Offsets {
		o, on := binary.Uvarint(buf[m:])
		n.Offsets[i] = o
		m += on
	}

	return nil
}

func (n *BTreeNode) WriteTo(w io.Writer) (int64, error) {
	buf, err := n.MarshalBinary()
	if err != nil {
		return 0, err
	}
	m, err := w.Write(buf)
	return int64(m), err
}

func (n *BTreeNode) Leaf() bool {
	return len(n.Offsets) == 0
}
