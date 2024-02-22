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
	leafPointers     []MemoryPointer
	internalPointers []uint64
	Keys             []ReferencedValue
}

func (n *BPTreeNode) Leaf() bool {
	return len(n.leafPointers) > 0
}

func (n *BPTreeNode) Pointer(i int) MemoryPointer {
	if n.Leaf() {
		return n.leafPointers[i]
	}
	return MemoryPointer{Offset: n.internalPointers[i]}
}

func (n *BPTreeNode) NumPointers() int {
	return len(n.internalPointers) + len(n.leafPointers)
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
	if n.Leaf() {
		binary.LittleEndian.PutUint32(buf[:4], uint32(-size))
	} else {
		binary.LittleEndian.PutUint32(buf[:4], uint32(size))
	}
	if size == 0 {
		panic("writing empty node")
	}
	ct := 4
	for _, k := range n.Keys {
		if k.DataPointer.Length > 0 {
			binary.LittleEndian.PutUint32(buf[ct:ct+4], ^uint32(0))
			binary.LittleEndian.PutUint64(buf[ct+4:ct+12], k.DataPointer.Offset)
			binary.LittleEndian.PutUint32(buf[ct+12:ct+16], k.DataPointer.Length)
			ct += 4 + 12
		} else {
			binary.LittleEndian.PutUint32(buf[ct:ct+4], uint32(len(k.Value)))
			m := copy(buf[ct+4:ct+4+len(k.Value)], k.Value)
			if m != len(k.Value) {
				return nil, fmt.Errorf("failed to copy key: %w", io.ErrShortWrite)
			}
			ct += m + 4
		}
	}
	for _, p := range n.leafPointers {
		binary.LittleEndian.PutUint64(buf[ct:ct+8], p.Offset)
		binary.LittleEndian.PutUint32(buf[ct+8:ct+12], p.Length)
		ct += 12
	}
	for _, p := range n.internalPointers {
		binary.LittleEndian.PutUint64(buf[ct:ct+8], p)
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
	size := int32(binary.LittleEndian.Uint32(buf[:4]))
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
		l := binary.LittleEndian.Uint32(buf[m : m+4])
		if l == ^uint32(0) {
			// read the key out of the memory pointer stored at this position
			n.Keys[i].DataPointer.Offset = binary.LittleEndian.Uint64(buf[m+4 : m+12])
			n.Keys[i].DataPointer.Length = binary.LittleEndian.Uint32(buf[m+12 : m+16])
			dp := n.Keys[i].DataPointer
			n.Keys[i].Value = n.DataParser.Parse(n.Data[dp.Offset : dp.Offset+uint64(dp.Length)]) // resolving the data-file
			m += 4 + 12
		} else {
			n.Keys[i].Value = buf[m+4 : m+4+int(l)]
			m += 4 + int(l)
		}
	}
	for i := range n.leafPointers {
		n.leafPointers[i].Offset = binary.LittleEndian.Uint64(buf[m : m+8])
		n.leafPointers[i].Length = binary.LittleEndian.Uint32(buf[m+8 : m+12])
		m += 12
	}
	for i := range n.internalPointers {
		n.internalPointers[i] = binary.LittleEndian.Uint64(buf[m : m+8])
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
