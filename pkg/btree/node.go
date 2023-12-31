package btree

import (
	"io"

	"github.com/kevmo314/appendable/pkg/encoding"
)

type Node struct {
	Size     uint8
	Keys     [8]DataPointer
	Children [9]uint64
	Leaf     bool
}

type DataPointer struct {
	RecordOffset        uint64
	FieldOffset, Length uint32
}

func (n *Node) encode(w io.Writer) error {
	size := n.Size
	if n.Leaf {
		// mark the first bit
		size |= 1 << 7
	}
	if err := encoding.WriteUint8(w, size); err != nil {
		return err
	}
	for i := 0; i < int(n.Size); i++ {
		if err := encoding.WriteUint64(w, n.Keys[i].RecordOffset); err != nil {
			return err
		}
		if err := encoding.WriteUint32(w, n.Keys[i].FieldOffset); err != nil {
			return err
		}
		if err := encoding.WriteUint32(w, n.Keys[i].Length); err != nil {
			return err
		}
	}
	for i := 0; i < int(n.Size)+1; i++ {
		if err := encoding.WriteUint64(w, n.Children[i]); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) decode(r io.Reader) error {
	size, err := encoding.ReadByte(r)
	if err != nil {
		return err
	}
	n.Size = size & 0x7f
	n.Leaf = size&(1<<7) != 0
	for i := 0; i < int(n.Size); i++ {
		recordOffset, err := encoding.ReadUint64(r)
		if err != nil {
			return err
		}
		fieldOffset, err := encoding.ReadUint32(r)
		if err != nil {
			return err
		}
		length, err := encoding.ReadUint32(r)
		if err != nil {
			return err
		}
		n.Keys[i] = DataPointer{
			RecordOffset: recordOffset,
			FieldOffset:  fieldOffset,
			Length:       length,
		}
	}
	for i := 0; i < int(n.Size)+1; i++ {
		child, err := encoding.ReadUint64(r)
		if err != nil {
			return err
		}
		n.Children[i] = child
	}
	return nil
}
