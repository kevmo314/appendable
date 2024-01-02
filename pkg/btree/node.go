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

type BPTreeNode struct {
	// contains the offset of the child node or the offset of the record for leaf
	// if the node is a leaf, the last pointer is the offset of the next leaf
	Pointers []MemoryPointer
	Keys     [][]byte
}

func (n *BPTreeNode) leaf() bool {
	// leafs contain the same number of pointers as keys
	return len(n.Pointers) == len(n.Keys)
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
		if err := binary.Write(w, binary.BigEndian, uint32(len(k))); err != nil {
			return 0, err
		}
		m, err := w.Write(k)
		if err != nil {
			return 0, err
		}
		ct += m + 4
	}
	for _, p := range n.Pointers {
		if err := binary.Write(w, binary.BigEndian, p.Offset); err != nil {
			return 0, err
		}
		if err := binary.Write(w, binary.BigEndian, p.Length); err != nil {
			return 0, err
		}
		ct += 12
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
		n.Keys = make([][]byte, -size)
	} else {
		n.Pointers = make([]MemoryPointer, size+1)
		n.Keys = make([][]byte, size)
	}
	m := 4
	for i := range n.Keys {
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return 0, err
		}
		n.Keys[i] = make([]byte, l)
		if _, err := io.ReadFull(r, n.Keys[i]); err != nil {
			return 0, err
		}
		m += 4 + int(l)
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
		cmp := bytes.Compare(key, n.Keys[m])
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
