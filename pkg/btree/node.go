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
	size := n.Size
	if n.Leaf {
		// mark the first bit
		size |= 1 << 7
	}
	if err := encoding.WriteUint8(w, size); err != nil {
		return 0, err
	}
	for i := 0; i < int(n.Size); i++ {
		if err := encoding.WriteUint64(w, n.Keys[i].RecordOffset); err != nil {
			return 0, err
		}
		if err := encoding.WriteUint32(w, n.Keys[i].FieldOffset); err != nil {
			return 0, err
		}
		if err := encoding.WriteUint32(w, n.Keys[i].Length); err != nil {
			return 0, err
		}
	}
	for i := 0; i < int(n.Size)+1; i++ {
		if err := encoding.WriteUint64(w, n.Children[i]); err != nil {
			return 0, err
		}
	}
	return 1 + 16*int64(n.Size) + 8*int64(n.Size+1), nil
}

func (n *Node) ReadFrom(r io.Reader) (int64, error) {
	size, err := encoding.ReadByte(r)
	if err != nil {
		return 0, err
	}
	n.Size = size & 0x7f
	n.Leaf = size&(1<<7) != 0
	for i := 0; i < int(n.Size); i++ {
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
	for i := 0; i < int(n.Size)+1; i++ {
		child, err := encoding.ReadUint64(r)
		if err != nil {
			return 0, err
		}
		n.Children[i] = child
	}
	return 1 + 16*int64(n.Size) + 8*int64(n.Size+1), nil
}
