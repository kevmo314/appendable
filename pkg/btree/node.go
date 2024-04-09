package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"math/bits"
)

type ReferencedValue struct {
	// it is generally optional to set the DataPointer. if it is not set, the
	// value is taken to be unreferenced and is stored directly in the node.
	// if it is set, the value is used for comparison but the value is stored
	// as a reference to the DataPointer.
	//
	// caveat: DataPointer is used as a disambiguator for the value. the b+ tree
	// implementation does not support duplicate keys and uses the DataPointer
	// to disambiguate between keys that compare as equal.
	DataPointer pointer.MemoryPointer
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
	LeafPointers     []pointer.MemoryPointer
	InternalPointers []uint64
	Keys             []ReferencedValue

	// the expected width for the BPtree's type
	Width uint16
}

func (n *BPTreeNode) Leaf() bool {
	return len(n.LeafPointers) > 0
}

func (n *BPTreeNode) Pointer(i int) pointer.MemoryPointer {
	if n.Leaf() {
		return n.LeafPointers[i]
	}
	return pointer.MemoryPointer{Offset: n.InternalPointers[i]}
}

func (n *BPTreeNode) NumPointers() int {
	return len(n.InternalPointers) + len(n.LeafPointers)
}

func SizeVariant(v uint64) int {
	return int(9*uint32(bits.Len64(v))+64) / 64
}

func (n *BPTreeNode) Size() int64 {

	size := 4 // number of keys
	var pk ReferencedValue
	for i, ck := range n.Keys {
		if i == 0 {
			pk = ck
		} else {
			if !bytes.Equal(pk.Value, ck.Value) || i == len(n.Keys)-1 {
				size++

				o := SizeVariant(pk.DataPointer.Offset)
				l := SizeVariant(uint64(pk.DataPointer.Length))
				size += l + o

				if n.Width != uint16(0) {
					size += len(pk.Value)
				}
			}

			if i == len(n.Keys)-1 && !bytes.Equal(pk.Value, ck.Value) {
				size++

				o := SizeVariant(ck.DataPointer.Offset)
				l := SizeVariant(uint64(ck.DataPointer.Length))
				size += l + o

				if n.Width != uint16(0) {
					size += len(ck.Value)
				}
			}

			pk = ck
		}
	}

	for _, n := range n.LeafPointers {
		o := SizeVariant(n.Offset)
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

	var pk ReferencedValue
	count := uint8(0)
	for i, ck := range n.Keys {
		if i == 0 {
			pk = ck
			count++
		} else {
			if bytes.Equal(pk.Value, ck.Value) {
				count++
			}

			// processing previous key (pk)
			if !bytes.Equal(pk.Value, ck.Value) || i == len(n.Keys)-1 {
				if count > 1 {
					buf[ct] = count | 0x80
				} else {
					buf[ct] = 0x01 // single occurrence
				}
				ct++
				on := binary.PutUvarint(buf[ct:], pk.DataPointer.Offset)
				ln := binary.PutUvarint(buf[ct+on:], uint64(pk.DataPointer.Length))
				ct += on + ln
				if n.Width != uint16(0) {
					m := copy(buf[ct:], pk.Value)
					if m != len(pk.Value) {
						return nil, fmt.Errorf("failed to copy key: %w", io.ErrShortWrite)
					}
					ct += m
				}

				count = 1
			}

			// processing current key (ck)
			if i == len(n.Keys)-1 && !bytes.Equal(pk.Value, ck.Value) {
				fmt.Printf("\nwriting key: %v at %v", ck.Value, i)
				buf[ct] = 0x01
				fmt.Printf("\nadding single occurence\n")
				ct++
				on := binary.PutUvarint(buf[ct:], ck.DataPointer.Offset)
				ln := binary.PutUvarint(buf[ct+on:], uint64(ck.DataPointer.Length))
				ct += on + ln
				if n.Width != 0 {
					m := copy(buf[ct:], ck.Value)
					if m != len(ck.Value) {
						return nil, fmt.Errorf("failed to copy key: %w", io.ErrShortWrite)
					}
					ct += m
				}
			}

			pk = ck
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
	if size == 0 {
		panic("empty node")
	}

	leaf := size < 0
	if leaf {
		n.LeafPointers = make([]pointer.MemoryPointer, -size)
		size = -size
	} else {
		n.InternalPointers = make([]uint64, size+1)
	}
	n.Keys = make([]ReferencedValue, 0, size)

	m := 4

	for m < len(buf) {
		var numIter uint8 = 1
		if buf[m]&0x80 != 0x00 {
			numIter = buf[m] & 0x7F
			fmt.Printf("multiple %v occ", numIter)
			m++
		} else if buf[m] == 0x01 {
			numIter = 1
			fmt.Printf("single occ\n")
			m++
		}

		o, on := binary.Uvarint(buf[m:])
		l, ln := binary.Uvarint(buf[m+on:])
		m += on + ln

		var keyValue []byte
		if n.Width == 0 {
			keyValue = n.DataParser.Parse(n.Data[o : o+l])
		} else {
			keyValue = make([]byte, n.Width-1)
			copy(keyValue, buf[m:m+int(n.Width-1)])
			m += int(n.Width - 1)
		}

		for j := uint8(0); j < numIter; j++ {
			n.Keys = append(n.Keys, ReferencedValue{
				DataPointer: pointer.MemoryPointer{
					Offset: o,
					Length: uint32(l),
				},
				Value: keyValue,
			})
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
