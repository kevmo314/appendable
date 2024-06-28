package btree

import (
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"math"
)

type BTreeNode struct {
	Ids     []pointer.ReferencedId
	Vectors []hnsw.Point

	Offsets   []uint64
	Width     uint16
	VectorDim uint64
}

func (n *BTreeNode) Size() int64 {
	size := 4

	for _, k := range n.Ids {
		size += encoding.SizeVarint(k.DataPointer.Offset)
		size += encoding.SizeVarint(uint64(k.DataPointer.Length))
		size += encoding.SizeVarint(uint64(k.Value))
	}

	for _, n := range n.Offsets {
		size += encoding.SizeVarint(n)
	}

	size += encoding.SizeVarint(n.VectorDim)
	size += len(n.Vectors) * (4 * int(n.VectorDim))

	return int64(size)
}

func (n *BTreeNode) Leaf() bool {
	return len(n.Offsets) == 0
}

func (n *BTreeNode) MarshalBinary() ([]byte, error) {
	size := int32(len(n.Ids))

	if size == 0 {
		panic("writing empty node, no ids found!")
	}

	buf := make([]byte, n.Size())

	if n.Leaf() {
		binary.LittleEndian.PutUint32(buf[:4], uint32(-size))
	} else {
		binary.LittleEndian.PutUint32(buf[:4], uint32(size))
	}

	ct := 4
	for _, k := range n.Ids {
		on := binary.PutUvarint(buf[ct:], k.DataPointer.Offset)
		ln := binary.PutUvarint(buf[ct+on:], uint64(k.DataPointer.Length))
		vn := binary.PutUvarint(buf[ct+on+ln:], uint64(k.Value))
		ct += on + ln + vn
	}

	for _, n := range n.Offsets {
		on := binary.PutUvarint(buf[ct:], n)
		ct += on
	}

	vdn := binary.PutUvarint(buf[ct:], n.VectorDim)
	ct += vdn

	for _, v := range n.Vectors {
		for _, elem := range v {
			binary.LittleEndian.PutUint32(buf[ct:], math.Float32bits(elem))
			ct += 4
		}
	}

	if ct != int(n.Size()) {
		panic(fmt.Sprintf("size mismatch. ct: %v, size: %v", ct, n.Size()))
	}

	return buf, nil
}

func (n *BTreeNode) UnmarshalBinary(buf []byte) error {
	size := int32(binary.LittleEndian.Uint32(buf[:4]))
	leaf := size < 0

	if leaf {
		n.Ids = make([]pointer.ReferencedId, -size)
		n.Vectors = make([]hnsw.Point, -size)
		n.Offsets = make([]uint64, 0)
	} else {
		n.Ids = make([]pointer.ReferencedId, size)
		n.Vectors = make([]hnsw.Point, size)
		n.Offsets = make([]uint64, size+1)
	}

	if size == 0 {
		panic("empty node")
	}

	m := 4
	for i := range n.Ids {
		o, on := binary.Uvarint(buf[m:])
		l, ln := binary.Uvarint(buf[m+on:])

		n.Ids[i].DataPointer.Offset = o
		n.Ids[i].DataPointer.Length = uint32(l)

		m += on + ln

		v, vn := binary.Uvarint(buf[m:])
		n.Ids[i].Value = hnsw.Id(v)

		m += vn
	}

	if !leaf {
		for i := range n.Offsets {
			o, on := binary.Uvarint(buf[m:])
			n.Offsets[i] = o
			m += on
		}
	}

	vecdim, vdn := binary.Uvarint(buf[m:])
	n.VectorDim = vecdim
	m += vdn

	for i := range n.Vectors {
		vector := make(hnsw.Point, vecdim)

		for vi := range vector {
			vector[vi] = float32(binary.LittleEndian.Uint32(buf[m:]))
			m += 4
		}

		n.Vectors[i] = vector
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
