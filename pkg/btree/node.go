package btree

import (
	"github.com/kevmo314/appendable/pkg/hnsw"
	"io"
)

type BTreeNode struct {
	Ids     []hnsw.Id
	Vectors []hnsw.Point

	Pointers []uint64
	Width    uint16
}

func (n *BTreeNode) Size() int64 {
	return 0
}

// MarshalBinary TODO!
func (n *BTreeNode) MarshalBinary() ([]byte, error) {
	b := []byte{}

	return b, nil
}

// UnmarshalBinary TODO!
func (n *BTreeNode) UnmarshalBinary(buf []byte) error {
	return nil
}

func (n *BTreeNode) WriteTo(w io.Writer) (int64, error) {
	buf, err := n.MarshalBinary()
	if err != nil {
		return 0, err
	}
	m, err := w.Write(buf)
	return int64(m), err
}
