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
	if err != nil || mp.Length == 0 {
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

func (t *BTree) Insert(key pointer.ReferencedId, vector hnsw.Point) error {
	root, rootOffset, err := t.root()
	if err != nil {
		return fmt.Errorf("read root node: %w", err)
	}

	if root == nil {
		node := &BTreeNode{Width: t.Width}
		node.Keys = []pointer.ReferencedId{key}
		node.Vectors = []hnsw.Point{vector}

		buf, err := node.MarshalBinary()
		if err != nil {
			return err
		}
		offset, err := t.PageFile.NewPage(buf)
		if err != nil {
			return err
		}
		return t.MetaPage.SetRoot(pointer.MemoryPointer{Offset: uint64(offset), Length: uint32(len(buf))})
	}

	parent, parentOffset := root, rootOffset.Offset
	for !parent.Leaf() {
		index, found := slices.BinarySearchFunc(parent.Keys, key, pointer.CompareReferencedIds)

		if found {
			panic("cannot insert duplicate key")
		}

		loffset := parent.Offsets[index]
		child, err := t.readNode(loffset)
		if err != nil {
			return err
		}

		if int(child.Size()) > t.PageFile.PageSize() {
			// split node here
			mid := len(child.Keys) / 2
			midKey := child.Keys[mid]

			rightChild := &BTreeNode{Width: t.Width}
			if !child.Leaf() {
				rightChild.Offsets = child.Offsets[mid+1:]
				child.Offsets = child.Offsets[:mid]
			}
			rightChild.Vectors = child.Vectors[mid+1:]
			rightChild.Keys = child.Keys[mid+1:]

			rbuf, err := rightChild.MarshalBinary()
			if err != nil {
				return err
			}
			roffset, err := t.PageFile.NewPage(rbuf)
			if err != nil {
				return err
			}

			// shrink left child (child)
			child.Keys = child.Keys[:mid]
			child.Vectors = child.Vectors[:mid]
			if _, err := t.PageFile.Seek(int64(loffset), io.SeekStart); err != nil {
				return err
			}

			if _, err := child.WriteTo(t.PageFile); err != nil {
				return err
			}

			// update parent to include new key and store left right offsets
			if index == len(parent.Keys) {
				parent.Keys = append(parent.Keys, midKey)
			} else {
				parent.Keys = append(parent.Keys[:index+1], parent.Keys[index:]...)
				parent.Keys[index] = midKey
			}

			parent.Offsets = append(parent.Offsets[:index+2], parent.Offsets[:index+1]...)
			parent.Offsets[index+1] = uint64(roffset)
			if _, err := t.PageFile.Seek(int64(parentOffset), io.SeekStart); err != nil {
				return err
			}
			if _, err := parent.WriteTo(t.PageFile); err != nil {
				return err
			}

			if pointer.CompareReferencedIds(midKey, key) == 1 {
				parent, parentOffset = child, loffset
			} else {
				parent, parentOffset = rightChild, uint64(roffset)
			}
		} else {
			if _, err := t.PageFile.Seek(int64(parentOffset), io.SeekStart); err != nil {
				return err
			}
			if _, err := parent.WriteTo(t.PageFile); err != nil {
				return err
			}
			parent, parentOffset = child, loffset
		}
	}

	index, found := slices.BinarySearchFunc(parent.Keys, key, pointer.CompareReferencedIds)
	if found {
		panic("cannot insert duplicate key")
	}

	parent.Keys = append(parent.Keys[:index+1], parent.Keys[index:]...)
	parent.Keys[index] = key

	parent.Vectors = append(parent.Vectors[:index+1], parent.Vectors[index:]...)
	parent.Vectors[index] = vector

	if _, err := t.PageFile.Seek(int64(parentOffset), io.SeekStart); err != nil {
		return err
	}
	if _, err := parent.WriteTo(t.PageFile); err != nil {
		return err
	}

	return nil
}

func (t *BTree) Find(key pointer.ReferencedId) (pointer.ReferencedId, pointer.MemoryPointer, error) {
	node, _, err := t.root()
	if err != nil {
		return pointer.ReferencedId{}, pointer.MemoryPointer{}, err
	}

	for {
		if node == nil {
			return pointer.ReferencedId{}, pointer.MemoryPointer{}, nil
		}

		index, found := slices.BinarySearchFunc(node.Keys, key, pointer.CompareReferencedIds)

		if found {
			return node.Keys[index-1], pointer.MemoryPointer{Offset: node.Offsets[index]}, nil
		}

		// no key found
		if node.Leaf() {
			return pointer.ReferencedId{}, pointer.MemoryPointer{}, nil
		}

		newOffset := node.Offsets[index]
		newNode, err := t.readNode(newOffset)
		if err != nil {
			return pointer.ReferencedId{}, pointer.MemoryPointer{}, err
		}

		node = newNode
	}
}
