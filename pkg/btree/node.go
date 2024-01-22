package btree

import (
	"bytes"
	"encoding/binary"
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
	Data io.ReaderAt
	// contains the offset of the child node or the offset of the record for leaf
	// if the node is a leaf, the last pointer is the offset of the next leaf
	Pointers []MemoryPointer
	Keys     []ReferencedValue
}

func (n *BPTreeNode) leaf() bool {
	// leafs contain the same number of pointers as keys
	return len(n.Pointers) == len(n.Keys)
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
	for range n.Pointers {
		size += 12
	}
	return int64(size)
}

func (n *BPTreeNode) WriteTo(w io.Writer) (int64, error) {
	size := int32(len(n.Keys))
	// set the first bit to 1 if it's a leaf
	if n.leaf() {
		if err := binary.Write(w, binary.BigEndian, -size); err != nil {
			return 0, err
		}
	} else {
		if err := binary.Write(w, binary.BigEndian, size); err != nil {
			return 0, err
		}
	}
	ct := 4
	for _, k := range n.Keys {
		if k.DataPointer.Length > 0 {
			if err := binary.Write(w, binary.BigEndian, uint32(0)); err != nil {
				return 0, err
			}
			if err := binary.Write(w, binary.BigEndian, k.DataPointer); err != nil {
				return 0, err
			}
			ct += 4 + 12
		} else {
			if err := binary.Write(w, binary.BigEndian, uint32(len(k.Value))); err != nil {
				return 0, err
			}
			m, err := w.Write(k.Value)
			if err != nil {
				return 0, err
			}
			ct += m + 4
		}
	}
	for _, p := range n.Pointers {
		if err := binary.Write(w, binary.BigEndian, p); err != nil {
			return 0, err
		}
		ct += 12
	}
	if ct != int(n.Size()) {
		panic("size mismatch")
	}
	return int64(ct), nil
}

func (n *BPTreeNode) ReadFrom(r io.Reader) (int64, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}
	leaf := size < 0
	if leaf {
		n.Pointers = make([]MemoryPointer, -size)
		n.Keys = make([]ReferencedValue, -size)
	} else {
		n.Pointers = make([]MemoryPointer, size+1)
		n.Keys = make([]ReferencedValue, size)
	}
	m := 4
	for i := range n.Keys {
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return 0, err
		}
		if l == 0 {
			// read the key out of the memory pointer stored at this position
			if err := binary.Read(r, binary.BigEndian, n.Keys[i].DataPointer); err != nil {
				return 0, err
			}
			n.Keys[i].Value = make([]byte, n.Keys[i].DataPointer.Length)
			if _, err := n.Data.ReadAt(n.Keys[i].Value, int64(n.Keys[i].DataPointer.Offset)); err != nil {
				return 0, err
			}
			m += 4 + 12
		} else {
			n.Keys[i].Value = make([]byte, l)
			if _, err := io.ReadFull(r, n.Keys[i].Value); err != nil {
				return 0, err
			}
			m += 4 + int(l)
		}
	}
	for i := range n.Pointers {
		if err := binary.Read(r, binary.BigEndian, &n.Pointers[i].Offset); err != nil {
			return 0, err
		}
		if err := binary.Read(r, binary.BigEndian, &n.Pointers[i].Length); err != nil {
			return 0, err
		}
		m += 12
	}
	return int64(m), nil
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
