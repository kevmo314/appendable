package btree

import (
	"io"

	"github.com/kevmo314/appendable/pkg/encoding"
)

type Node struct {
	Keys     []DataPointer
	Children []uint64
	Leaf     bool
}

type DataPointer struct {
	RecordOffset        uint64
	FieldOffset, Length uint32
}

func (p DataPointer) Value(r io.ReadSeeker) ([]byte, error) {
	buf := make([]byte, p.Length)
	if _, err := r.Seek(int64(p.RecordOffset+uint64(p.FieldOffset)), io.SeekStart); err != nil {
		return nil, err
	}
	n, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != int(p.Length) {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func (n *Node) WriteTo(w io.Writer) (int64, error) {
	size := len(n.Keys)
	if n.Leaf {
		// mark the first bit
		size |= 1 << 7
	}
	if err := encoding.WriteUint8(w, uint8(size)); err != nil {
		return 0, err
	}
	for _, key := range n.Keys {
		if err := encoding.WriteUint64(w, key.RecordOffset); err != nil {
			return 0, err
		}
		if err := encoding.WriteUint32(w, key.FieldOffset); err != nil {
			return 0, err
		}
		if err := encoding.WriteUint32(w, key.Length); err != nil {
			return 0, err
		}
	}
	if !n.Leaf {
		for _, child := range n.Children {
			if err := encoding.WriteUint64(w, child); err != nil {
				return 0, err
			}
		}
	}
	return int64(1 + 16*len(n.Keys) + 8*len(n.Children)), nil
}

func (n *Node) ReadFrom(r io.Reader) (int64, error) {
	size, err := encoding.ReadUint8(r)
	if err != nil {
		return 0, err
	}
	n.Leaf = size&(1<<7) != 0
	size = size & (1<<7 - 1)
	n.Keys = make([]DataPointer, size)
	for i := 0; i < int(size); i++ {
		recordOffset, err := encoding.ReadUint64(r)
		if err != nil {
			return 0, err
		}
		fieldOffset, err := encoding.ReadUint32(r)
		if err != nil {
			return 0, err
		}
		length, err := encoding.ReadUint32(r)
		if err != nil {
			return 0, err
		}
		n.Keys[i] = DataPointer{
			RecordOffset: recordOffset,
			FieldOffset:  fieldOffset,
			Length:       length,
		}
	}
	if !n.Leaf {
		n.Children = make([]uint64, size+1)
		for i := 0; i <= int(size); i++ {
			child, err := encoding.ReadUint64(r)
			if err != nil {
				return 0, err
			}
			n.Children[i] = child
		}
	}
	return 1 + 16*int64(size) + 8*int64(size+1), nil
}

func (n *Node) Clone() *Node {
	return &Node{
		Keys:     n.Keys[:],
		Children: n.Children[:],
		Leaf:     n.Leaf,
	}
}
