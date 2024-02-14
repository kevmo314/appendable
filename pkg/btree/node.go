package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MemoryPointer is a uint64 offset and uint32 length
type MemoryPointer struct {
	Offset uint64
	Length uint32
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

func CompareReferencedValues(a, b ReferencedValue) int {
	cmp := bytes.Compare(a.Value, b.Value)
	if cmp != 0 {
		return cmp
	}
	if a.DataPointer.Offset != b.DataPointer.Offset {
		return int(a.DataPointer.Offset - b.DataPointer.Offset)
	}
	return int(a.DataPointer.Length - b.DataPointer.Length)
}

type BPTreeNode struct {
	Data []byte
	// contains the offset of the child node or the offset of the record for leaf
	// if the node is a leaf, the last pointer is the offset of the next leaf
	leafPointers     []MemoryPointer
	internalPointers []uint64
	Keys             []ReferencedValue
}

func (n *BPTreeNode) leaf() bool {
	return len(n.leafPointers) > 0
}

func (n *BPTreeNode) Pointers() []MemoryPointer {
	if n.leaf() {
		return n.leafPointers
	}
	pointers := make([]MemoryPointer, len(n.internalPointers))
	for i, p := range n.internalPointers {
		pointers[i].Offset = p
	}
	return pointers
}

func (n *BPTreeNode) Pointer(i int) MemoryPointer {
	if n.leaf() {
		return n.leafPointers[(len(n.leafPointers)+i)%len(n.leafPointers)]
	}
	return MemoryPointer{Offset: n.internalPointers[(len(n.internalPointers)+i)%len(n.internalPointers)]}
}

func (n *BPTreeNode) Size() int64 {
	size := 4 // number of keys
	for _, k := range n.Keys {
		if k.DataPointer.Length > 0 {
			size += 4 + 12 // length of key + length of pointer
		} else {
			size += 4 + len(k.Value)
		}
	}
	for range n.leafPointers {
		size += 12
	}
	for range n.internalPointers {
		size += 8
	}
	return int64(size)
}

func (n *BPTreeNode) MarshalBinary() ([]byte, error) {
	size := int32(len(n.Keys))
	buf := make([]byte, n.Size())
	// set the first bit to 1 if it's a leaf
	if n.leaf() {
		binary.BigEndian.PutUint32(buf[:4], uint32(-size))
	} else {
		binary.BigEndian.PutUint32(buf[:4], uint32(size))
	}
	if size == 0 {
		panic("writing empty node")
	}
	ct := 4
	for _, k := range n.Keys {
		if k.DataPointer.Length > 0 {
			binary.BigEndian.PutUint32(buf[ct:ct+4], ^uint32(0))
			binary.BigEndian.PutUint64(buf[ct+4:ct+12], k.DataPointer.Offset)
			binary.BigEndian.PutUint32(buf[ct+12:ct+16], k.DataPointer.Length)
			ct += 4 + 12
		} else {
			binary.BigEndian.PutUint32(buf[ct:ct+4], uint32(len(k.Value)))
			m := copy(buf[ct+4:ct+4+len(k.Value)], k.Value)
			if m != len(k.Value) {
				return nil, fmt.Errorf("failed to copy key: %w", io.ErrShortWrite)
			}
			ct += m + 4
		}
	}
	for _, p := range n.leafPointers {
		binary.BigEndian.PutUint64(buf[ct:ct+8], p.Offset)
		binary.BigEndian.PutUint32(buf[ct+8:ct+12], p.Length)
		ct += 12
	}
	for _, p := range n.internalPointers {
		binary.BigEndian.PutUint64(buf[ct:ct+8], p)
		ct += 8
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
	size := int32(binary.BigEndian.Uint32(buf[:4]))
	leaf := size < 0
	if leaf {
		n.leafPointers = make([]MemoryPointer, -size)
		n.Keys = make([]ReferencedValue, -size)
	} else {
		n.internalPointers = make([]uint64, size+1)
		n.Keys = make([]ReferencedValue, size)
	}
	if size == 0 {
		panic("empty node")
	}

	m := 4
	for i := range n.Keys {
		l := binary.BigEndian.Uint32(buf[m : m+4])
		if l == ^uint32(0) {
			// read the key out of the memory pointer stored at this position
			n.Keys[i].DataPointer.Offset = binary.BigEndian.Uint64(buf[m+4 : m+12])
			n.Keys[i].DataPointer.Length = binary.BigEndian.Uint32(buf[m+12 : m+16])
			dp := n.Keys[i].DataPointer
			n.Keys[i].Value = n.Data[dp.Offset : dp.Offset+uint64(dp.Length)]
			m += 4 + 12
		} else {
			n.Keys[i].Value = buf[m+4 : m+4+int(l)]
			m += 4 + int(l)
		}
	}
	for i := range n.leafPointers {
		n.leafPointers[i].Offset = binary.BigEndian.Uint64(buf[m : m+8])
		n.leafPointers[i].Length = binary.BigEndian.Uint32(buf[m+8 : m+12])
		m += 12
	}
	for i := range n.internalPointers {
		n.internalPointers[i] = binary.BigEndian.Uint64(buf[m : m+8])
		m += 8
	}
	return nil
}

func (n *BPTreeNode) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, pageSizeBytes)
	if _, err := r.Read(buf); err != nil && err != io.EOF {
		return 0, err
	}
	if err := n.UnmarshalBinary(buf); err != nil {
		return 0, err
	}
	return pageSizeBytes, nil
}
