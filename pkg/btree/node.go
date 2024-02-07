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
	DataPointer MemoryPointer
	Value       []byte
}

type BPTreeNode struct {
	Data []byte
	// contains the offset of the child node or the offset of the record for leaf
	// if the node is a leaf, the last pointer is the offset of the next leaf
	LeafPointers     []MemoryPointer
	InternalPointers []uint64
	Keys             []ReferencedValue
}

func (n *BPTreeNode) leaf() bool {
	// leafs contain the same number of pointers as keys
	return len(n.LeafPointers) == len(n.Keys)
}

func (n *BPTreeNode) Pointers() []MemoryPointer {
	if n.leaf() {
		return n.LeafPointers
	}

	pointers := make([]MemoryPointer, len(n.InternalPointers))

	for i, offset := range n.InternalPointers {
		pointers[i] = MemoryPointer{Offset: offset, Length: pageSizeBytes}
	}
	return pointers
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

	if n.leaf() {
		for range n.LeafPointers {
			size += 12
		}
	} else {
		for range n.InternalPointers {
			size += 8
		}
	}

	return int64(size)
}

func (n *BPTreeNode) MarshalBinary() ([]byte, error) {
	fmt.Println("unmarshall")
	size := int32(len(n.Keys))
	buf := make([]byte, n.Size())
	// set the first bit to 1 if it's a leaf
	if n.leaf() {
		binary.BigEndian.PutUint32(buf[:4], uint32(-size))

	} else {
		binary.BigEndian.PutUint32(buf[:4], uint32(size))
	}

	fmt.Printf("is leaf v, size %v", size)
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
	if n.leaf() {
		for _, p := range n.LeafPointers {
			binary.BigEndian.PutUint64(buf[ct:ct+8], p.Offset)
			binary.BigEndian.PutUint32(buf[ct+8:ct+12], p.Length)
			ct += 12
		}
	} else {
		for _, offset := range n.InternalPointers {
			binary.BigEndian.PutUint64(buf[ct:ct+8], offset)
			// For internal pointers, we do not store Length, as these pointers refer to whole pages.
			ct += 8
		}
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

	if leaf {
		for i := range n.LeafPointers {
			n.LeafPointers[i].Offset = binary.BigEndian.Uint64(buf[m : m+8])
			n.LeafPointers[i].Length = binary.BigEndian.Uint32(buf[m+8 : m+12])
			m += 12
		}
	} else {
		for i := range n.InternalPointers {
			n.InternalPointers[i] = binary.BigEndian.Uint64(buf[m : m+8])
			m += 8
		}
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

func (n *BPTreeNode) bsearch(key []byte) (int, bool) {
	i, j := 0, len(n.Keys)-1
	for i <= j {
		m := (i + j) / 2
		cmp := bytes.Compare(key, n.Keys[m].Value)
		if cmp == 0 {
			return m, true
		} else if cmp < 0 {
			j = m - 1
		} else {
			i = m + 1
		}
	}
	return i, false
}
