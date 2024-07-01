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
	MetaPage  metapage.MetaPage
	PageFile  pagefile.ReadWriteSeekPager
	VectorDim uint64
	Width     uint16
}

func NewBTree(metapage metapage.MetaPage, pf pagefile.ReadWriteSeekPager, vectorDim uint64) *BTree {
	return &BTree{MetaPage: metapage, PageFile: pf, VectorDim: vectorDim, Width: uint16(0)}
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

	node := &BTreeNode{Width: t.Width, VectorDim: t.VectorDim}
	buf := make([]byte, t.PageFile.PageSize())

	if _, err := t.PageFile.Read(buf); err != nil {
		return nil, err
	}

	if err := node.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	return node, nil
}

type TraversalRecord struct {
	node  *BTreeNode
	index int
	ptr   pointer.MemoryPointer
}

func (t *BTree) traverse(key pointer.ReferencedId, node *BTreeNode, ptr pointer.MemoryPointer) ([]TraversalRecord, error) {
	index, found := slices.BinarySearchFunc(node.Ids, key, pointer.CompareReferencedIds)

	if node.Leaf() {
		return []TraversalRecord{{node, index, ptr}}, nil
	}

	if found {
		index++
	}

	childOffset := node.Offsets[index]
	child, err := t.readNode(childOffset)
	if err != nil {
		return nil, err
	}

	path, err := t.traverse(key, child, pointer.MemoryPointer{Offset: childOffset})
	if err != nil {
		return nil, err
	}

	return append(path, TraversalRecord{
		node:  node,
		index: index,
		ptr:   ptr,
	}), nil
}

func (t *BTree) Insert(key pointer.ReferencedId, vector hnsw.Point) error {
	root, rootOffset, err := t.root()
	if err != nil {
		return fmt.Errorf("root: %w", err)
	}

	if root == nil {
		node := &BTreeNode{Width: t.Width, VectorDim: t.VectorDim}
		node.Ids = []pointer.ReferencedId{key}
		node.Vectors = []hnsw.Point{vector}
		node.Offsets = make([]uint64, 0)

		buf, err := node.MarshalBinary()
		if err != nil {
			return err
		}

		offset, err := t.PageFile.Write(buf)
		if err != nil {
			return err
		}

		return t.MetaPage.SetRoot(pointer.MemoryPointer{Offset: uint64(offset), Length: uint32(len(buf))})
	}

	path, err := t.traverse(key, root, rootOffset)
	if err != nil {
		return err
	}

	n := path[0].node
	j, found := slices.BinarySearchFunc(n.Ids, key, pointer.CompareReferencedIds)
	if found {
		return fmt.Errorf("key already exists. Data pointer: %v", key.DataPointer)
	}

	if j == len(n.Ids) {
		n.Ids = append(n.Ids, key)
		n.Vectors = append(n.Vectors, vector)
	} else {
		n.Ids = append(n.Ids[:j+1], n.Ids[j:]...)
		n.Ids[j] = key
		n.Vectors = append(n.Vectors[:j+1], n.Vectors[j:]...)
		n.Vectors[j] = vector
	}

	for i := 0; i < len(path); i++ {
		tr := path[i]
		n := tr.node
		if int(n.Size()) > t.PageFile.PageSize() {
			// split the node
			// mid is the key that will be inserted into the parent
			mid := len(n.Ids) / 2
			midKey := n.Ids[mid]
			midVector := n.Vectors[mid]

			// n is the left node, m the right node
			m := &BTreeNode{Width: t.Width, VectorDim: t.VectorDim}
			if n.Leaf() {
				m.Vectors = n.Vectors[mid:]
				m.Ids = n.Ids[mid:]
			} else {
				// for non-leaf nodes, the mid key is inserted into the parent
				m.Offsets = n.Offsets[mid+1:]
				m.Ids = n.Ids[mid+1:]
				m.Vectors = n.Vectors[mid+1:]
			}
			mbuf, err := m.MarshalBinary()
			if err != nil {
				return err
			}
			moffset, err := t.PageFile.NewPage(mbuf)
			if err != nil {
				return err
			}

			if n.Leaf() {
				n.Vectors = n.Vectors[:mid]
				n.Ids = n.Ids[:mid]
			} else {
				n.Offsets = n.Offsets[:mid+1]
				n.Vectors = n.Vectors[:mid]
				n.Ids = n.Ids[:mid]
			}

			noffset := tr.ptr.Offset
			if _, err := t.PageFile.Seek(int64(noffset), io.SeekStart); err != nil {
				return err
			}
			if _, err := n.WriteTo(t.PageFile); err != nil {
				return err
			}

			// update the parent
			if i < len(path)-1 {
				p := path[i+1]
				// insert the key into the parent
				if p.index == len(p.node.Ids) {
					p.node.Ids = append(p.node.Ids, midKey)
					p.node.Vectors = append(p.node.Vectors, midVector)
				} else {
					p.node.Ids = append(p.node.Ids[:p.index+1], p.node.Ids[p.index:]...)
					p.node.Ids[p.index] = midKey

					p.node.Vectors = append(p.node.Vectors[:p.index+1], p.node.Vectors[p.index:]...)
					p.node.Vectors[p.index] = midVector
				}
				p.node.Offsets = append(p.node.Offsets[:p.index+1], p.node.Offsets[p.index:]...)
				p.node.Offsets[p.index] = noffset
				p.node.Offsets[p.index+1] = uint64(moffset)
				// the parent will be written to disk in the next iteration
			} else {
				// the root split, so create a new root
				p := &BTreeNode{VectorDim: t.VectorDim, Width: t.Width}
				p.Ids = []pointer.ReferencedId{midKey}
				p.Vectors = []hnsw.Point{midVector}
				p.Offsets = []uint64{
					noffset, uint64(moffset),
				}

				pbuf, err := p.MarshalBinary()
				if err != nil {
					return err
				}
				poffset, err := t.PageFile.NewPage(pbuf)
				if err != nil {
					return err
				}
				if err := t.MetaPage.SetRoot(pointer.MemoryPointer{Offset: uint64(poffset), Length: uint32(len(pbuf))}); err != nil {
					return err
				}
				return nil
			}
		} else {
			// write this node to disk and update the parent
			if _, err := t.PageFile.Seek(int64(tr.ptr.Offset), io.SeekStart); err != nil {
				return err
			}
			if _, err := tr.node.WriteTo(t.PageFile); err != nil {
				return err
			}
			// no new nodes were produced, so we can return here
			return nil
		}
	}
	panic("unreachable")
}

func (t *BTree) Find(key pointer.ReferencedId) (pointer.ReferencedId, hnsw.Point, error) {
	root, rootOffset, err := t.root()
	if err != nil {
		return pointer.ReferencedId{}, hnsw.Point{}, err
	}

	path, err := t.traverse(key, root, rootOffset)
	if err != nil || len(path) == 0 {
		return pointer.ReferencedId{}, hnsw.Point{}, err
	}

	leaf := path[0].node
	j, found := slices.BinarySearchFunc(leaf.Ids, key, pointer.CompareReferencedIds)

	if found {
		return leaf.Ids[j], leaf.Vectors[j], nil
	}

	return pointer.ReferencedId{}, hnsw.Point{}, fmt.Errorf("key %v not found", key.Value)
}
