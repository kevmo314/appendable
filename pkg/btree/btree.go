package btree

import (
	"fmt"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/metapage"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
	"slices"
)

type BTree struct {
	MetaPage metapage.MetaPage
	PageFile pagefile.ReadWriteSeekPager

	Width uint16
}

func (t *BTree) root() (*BTreeNode, pointer.MemoryPointer, error) {
	mp, err := t.MetaPage.Root()
	if err != nil {
		return nil, mp, err
	}

	root, err := t.readNode(mp.Offset)
	if err != nil {
		return nil, mp, err
	}

	return root, mp, nil
}

func (t *BTree) readNode(offset uint64) (*BTreeNode, error) {
	if _, err := t.PageFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	node := &BTreeNode{Width: t.Width}
	buf := make([]byte, t.PageFile.PageSize())

	if _, err := t.PageFile.Read(buf); err != nil {
		return nil, err
	}

	if err := node.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	return node, nil
}

// Insert has the following assumptions:
// key.Value represents the Node Id. It is written to []bytes in LittleEndian.
func (t *BTree) Insert(key pointer.ReferencedValue, value hnsw.Point) error {
	root, _, err := t.root()
	if err != nil {
		return fmt.Errorf("read root node: %d", err)
	}

	if root == nil {
		node := &BTreeNode{
			Keys:    []pointer.ReferencedValue{key},
			Vectors: []hnsw.Point{value},
			Width:   t.Width,
		}
		buf, err := node.MarshalBinary()
		if err != nil {
			return err
		}

		offset, err := t.PageFile.NewPage(buf)
		if err != nil {
			return err
		}

		return t.MetaPage.SetRoot(pointer.MemoryPointer{
			Offset: uint64(offset),
			Length: uint32(len(buf)),
		})
	}

	parent := root
	for !parent.Leaf() {
		index, found := slices.BinarySearchFunc(parent.Keys, key, pointer.CompareReferencedValues)
		if found {
			index++
		}

		if len(parent.Pointers) > index {
			return fmt.Errorf("found index %d, but node.Pointers length is %d", index, len(parent.Pointers))
		}

		childPointer := parent.Pointers[index]
		child, err := t.readNode(childPointer)
		if err != nil {
			return err
		}

		if int(child.Size()) > t.PageFile.PageSize() {
			rightChild, midKey, err := t.SplitChild(parent, index, child)
			if err != nil {
				return err
			}

			switch pointer.CompareReferencedValues(midKey, key) {
			case 1:
				// key < midKey
				parent = child
			default:
				// right child
				parent = rightChild
			}
		} else {
			parent = child
		}
	}

	return nil
}

func (t *BTree) SplitChild(parent *BTreeNode, leftChildIndex int, leftChild *BTreeNode) (*BTreeNode, pointer.ReferencedValue, error) {
	mid := len(leftChild.Keys) / 2

	midKey, midVector := leftChild.Keys[mid], leftChild.Vectors[mid]

	rightChild := &BTreeNode{
		Keys:     append([]pointer.ReferencedValue(nil), leftChild.Keys[mid+1:]...),
		Vectors:  append([]hnsw.Point(nil), leftChild.Vectors[mid+1:]...),
		Pointers: append([]uint64(nil), leftChild.Pointers[mid+1:]...),
		Width:    t.Width,
	}

	rbuf, err := rightChild.MarshalBinary()
	if err != nil {
		return nil, pointer.ReferencedValue{}, err
	}
	roffset, err := t.PageFile.NewPage(rbuf)
	if err != nil {
		return nil, pointer.ReferencedValue{}, err
	}

	leftChild.Keys = leftChild.Keys[:mid]
	leftChild.Vectors = leftChild.Vectors[:mid]
	leftChild.Pointers = leftChild.Pointers[:mid]

	parent.Keys = append(parent.Keys[:leftChildIndex], append([]pointer.ReferencedValue{midKey}, parent.Keys[leftChildIndex:]...)...)
	parent.Vectors = append(parent.Vectors[:leftChildIndex], append([]hnsw.Point{midVector}, parent.Vectors[leftChildIndex:]...)...)
	parent.Pointers = append(parent.Pointers[:leftChildIndex+1], append([]uint64{uint64(roffset)}, parent.Pointers[leftChildIndex+1:]...)...)

	return rightChild, midKey, nil
}
