package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
)

// MemoryPointer is a uint64 offset and uint32 length
type MemoryPointer struct {
	Offset uint64
	Length uint32
}

func (mp MemoryPointer) String() string {
	return fmt.Sprintf("Pointer[%08x:%08x]", mp.Offset, mp.Offset+uint64(mp.Length))
}

type ReferencedValue struct {
	// it is generally optional to set the DataPointer. if it is not set, the
	// value is taken to be unreferenced and is stored directly in the node.
	// if it is set, the value is used for comparison but the value is stored
	// as a reference to the DataPointer.
	//
	// caveat: DataPointer is used as a disambiguator for the value. the b+ tree
	// implementation does not support duplicate keys and uses the DataPointer
	// to disambiguate between keys that compare as equal.
	DataPointer MemoryPointer
	Value       []byte
}

func (rv ReferencedValue) String() string {
	return fmt.Sprintf("ReferencedValue@%s{%s}", rv.DataPointer, rv.Value)
}

func CompareReferencedValues(a, b ReferencedValue) int {
	if cmp := bytes.Compare(a.Value, b.Value); cmp != 0 {
		return cmp
	} else if a.DataPointer.Offset < b.DataPointer.Offset {
		return -1
	} else if a.DataPointer.Offset > b.DataPointer.Offset {
		return 1
	} else if a.DataPointer.Length < b.DataPointer.Length {
		return -1
	} else if a.DataPointer.Length > b.DataPointer.Length {
		return 1
	}
	return 0
}

type DataParser interface {
	Parse([]byte) []byte
}

type BPTreeNode struct {
	Data       []byte
	DataParser DataParser
	// contains the offset of the child node or the offset of the record for leaf
	// if the node is a leaf, the last pointer is the offset of the next leaf
	LeafPointers     []MemoryPointer
	InternalPointers []uint64
	Keys             []ReferencedValue

	// the expected width for the BPtree's type
	Width uint16
}

func (n *BPTreeNode) Leaf() bool {
	return len(n.LeafPointers) > 0
}

func (n *BPTreeNode) Pointer(i int) MemoryPointer {
	if n.Leaf() {
		return n.LeafPointers[i]
	}
	return MemoryPointer{Offset: n.InternalPointers[i]}
}

func (n *BPTreeNode) NumPointers() int {
	return len(n.InternalPointers) + len(n.LeafPointers)
}

func SizeVariant(v uint64) int {
	return int(9*uint32(bits.Len64(v))+64) / 64
}

func (n *BPTreeNode) Size() int64 {

	size := 4 // number of keys
	for _, k := range n.Keys {
    o := SizeVariant(uint64(k.DataPointer.Offset))
		l := SizeVariant(uint64(k.DataPointer.Length))
		size += l + o

		if n.Width != uint16(0) {
			size += len(k.Value)
		}
	}
	for _, n := range n.LeafPointers {
    o := SizeVariant(uint64(n.Offset))
		l := SizeVariant(uint64(n.Length))
		size += o + l
	}
	for _, n := range n.InternalPointers {
		o := len(binary.AppendUvarint([]byte{}, n))
		size += o
	}
	return int64(size)
}

func (n *BPTreeNode) MarshalBinary() ([]byte, error) {
	size := int32(len(n.Keys))

	if size == 0 {
		panic("writing empty node")
	}
	buf := make([]byte, n.Size())
	// set the first bit to 1 if it's a leaf
	if n.Leaf() {
		binary.LittleEndian.PutUint32(buf[:4], uint32(-size))
	} else {
		binary.LittleEndian.PutUint32(buf[:4], uint32(size))
	}
	ct := 4
	for _, k := range n.Keys {
		on := binary.PutUvarint(buf[ct:], k.DataPointer.Offset)
		ln := binary.PutUvarint(buf[ct+on:], uint64(k.DataPointer.Length))
		ct += on + ln
		if n.Width != uint16(0) {
			m := copy(buf[ct:ct+len(k.Value)], k.Value)
			if m != len(k.Value) {
				return nil, fmt.Errorf("failed to copy key: %w", io.ErrShortWrite)
			}
			ct += m
		}
	}
	for _, p := range n.LeafPointers {
		on := binary.PutUvarint(buf[ct:], p.Offset)
		ln := binary.PutUvarint(buf[ct+on:], uint64(p.Length))

		ct += on + ln
	}
	for _, p := range n.InternalPointers {
		on := binary.PutUvarint(buf[ct:], p)
		ct += on
	}
	if ct != int(n.Size()) {
		panic("size mismatch")
	}
	return buf, nil
}

func (n *BPTreeNode) WriteTo(w io.Writer) (int64, error) {
	buf, err := n.MarshalBinary()
	if err != nil {
		return 0, err
	}
	m, err := w.Write(buf)
	return int64(m), err
}

func (n *BPTreeNode) UnmarshalBinary(buf []byte) error {
	size := int32(binary.LittleEndian.Uint32(buf[:4]))
	leaf := size < 0
	if leaf {
		n.LeafPointers = make([]MemoryPointer, -size)
		n.Keys = make([]ReferencedValue, -size)
	} else {
		n.InternalPointers = make([]uint64, size+1)
		n.Keys = make([]ReferencedValue, size)
	}
	if size == 0 {
		panic("empty node")
	}

	m := 4
	for i := range n.Keys {
		o, on := binary.Uvarint(buf[m:])
		l, ln := binary.Uvarint(buf[m+on:])

		n.Keys[i].DataPointer.Offset = o
		n.Keys[i].DataPointer.Length = uint32(l)

		m += on + ln

		if n.Width == uint16(0) {
			// read the key out of the memory pointer stored at this position
			dp := n.Keys[i].DataPointer
			n.Keys[i].Value = n.DataParser.Parse(n.Data[dp.Offset : dp.Offset+uint64(dp.Length)]) // resolving the data-file
		} else {
			n.Keys[i].Value = buf[m : m+int(n.Width-1)]
			m += int(n.Width - 1)
		}
	}
	for i := range n.LeafPointers {

		o, on := binary.Uvarint(buf[m:])
		l, ln := binary.Uvarint(buf[m+on:])

		n.LeafPointers[i].Offset = o
		n.LeafPointers[i].Length = uint32(l)
		m += on + ln
	}
	for i := range n.InternalPointers {
		o, on := binary.Uvarint(buf[m:])
		n.InternalPointers[i] = o
		m += on
	}
	return nil
}
