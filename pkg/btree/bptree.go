package btree

import (
	"bytes"
	"fmt"
	"io"
	"slices"
)

// MetaPage is an abstract interface over the root page of a btree
// This allows the caller to control the memory location of the meta
// pointer
type MetaPage interface {
	Root() (MemoryPointer, error)
	SetRoot(MemoryPointer) error
}

type BPTree struct {
	tree ReadWriteSeekPager
	meta MetaPage

	maxPageSize int

	Data []byte
}

func NewBPTree(tree ReadWriteSeekPager, meta MetaPage) *BPTree {
	return &BPTree{tree: tree, meta: meta}
}

func (t *BPTree) root() (*BPTreeNode, MemoryPointer, error) {
	mp, err := t.meta.Root()
	if err != nil || mp.Length == 0 {
		return nil, mp, nil
	}
	root, err := t.readNode(mp)
	if err != nil {
		return nil, mp, err
	}
	return root, mp, nil
}

type TraversalRecord struct {
	node  *BPTreeNode
	index int
	// the offset is useful so we know which page to free when we split
	ptr MemoryPointer
}

type TraversalPath struct {
	tree    *BPTree
	records []TraversalRecord
}

func (t *BPTree) Find(key []byte) (*TraversalPath, error) {
	root, rootOffset, err := t.root()
	if err != nil {
		return nil, fmt.Errorf("read root node: %w", err)
	}
	if root == nil {
		return nil, nil
	}
	records, err := t.traverse(key, root, rootOffset)
	if err != nil {
		return nil, err
	}
	return &TraversalPath{tree: t, records: records}, nil
}

func (tp *BPTree) FindFirst(key []byte) (MemoryPointer, bool, error) {
	path, err := tp.Find(key)
	if err != nil {
		return MemoryPointer{}, false, err
	}
	if path == nil {
		return MemoryPointer{}, false, nil
	}
	retrieved, value, err := path.Next()
	if err != nil || !bytes.Equal(key, retrieved) {
		return MemoryPointer{}, false, err
	}
	return value, true, nil
}

func (tp *TraversalPath) Next() ([]byte, MemoryPointer, error) {
	p := tp.records
	if len(p) == 0 {
		return nil, MemoryPointer{}, io.EOF
	}
	if p[0].index == len(p[0].node.Keys) {
		// this is a signal that there's no more data, however it's not an error.
		return nil, MemoryPointer{}, nil
	}
	key := p[0].node.Keys[p[0].index].Value
	value := p[0].node.Pointer(p[0].index)
	p[0].index++
	if p[0].index == len(p[0].node.Keys) {
		// propagate the carryover
		if len(p) == 1 {
			// we're at the end of the tree, no more data
			return key, value, nil
		}
		p[0].index = 0
		for i := 1; i < len(p); i++ {
			panic("incrementing parent!")
			p[i].index++
			if p[i].index > len(p[i].node.Keys) {
				if i == len(p)-1 {
					// we're at the end of the tree, no more data
					break
				}
				p[i].index = 0
			} else {
				// we found a node with keys left, so update the path
				for j := i - 1; j >= 0; j-- {
					node, err := tp.tree.readNode(p[j+1].node.Pointer(p[j+1].index))
					if err != nil {
						return nil, MemoryPointer{}, err
					}
					p[j].node = node
				}
				break
			}
		}
	}
	return key, value, nil
}

func (t *BPTree) readNode(ptr MemoryPointer) (*BPTreeNode, error) {
	if _, err := t.tree.Seek(int64(ptr.Offset), io.SeekStart); err != nil {
		return nil, err
	}
	node := &BPTreeNode{Data: t.Data}
	if _, err := node.ReadFrom(t.tree); err != nil {
		return nil, err
	}
	return node, nil
}

// traverse returns the path from root to leaf in reverse order (leaf first)
// the last element is always the node passed in
func (t *BPTree) traverse(key []byte, node *BPTreeNode, ptr MemoryPointer) ([]TraversalRecord, error) {
	// binary search node.Keys to find the first key greater than key (or gte if leaf)
	index, _ := slices.BinarySearchFunc(node.Keys, ReferencedValue{Value: key}, func(e ReferencedValue, t ReferencedValue) int {
		if cmp := bytes.Compare(e.Value, t.Value); cmp == 0 && !node.leaf() {
			return -1
		} else {
			return cmp
		}
	})

	if node.leaf() {
		return []TraversalRecord{{node: node, index: index, ptr: ptr}}, nil
	}

	child, err := t.readNode(node.Pointer(index))
	if err != nil {
		return nil, err
	}
	path, err := t.traverse(key, child, node.Pointer(index))
	if err != nil {
		return nil, err
	}
	return append(path, TraversalRecord{node: node, index: index, ptr: ptr}), nil
}

func (t *BPTree) Insert(key ReferencedValue, value MemoryPointer) error {
	root, rootOffset, err := t.root()
	if err != nil {
		return fmt.Errorf("read root node: %w", err)
	}
	if root == nil {
		// special case, create the root as the first node
		node := &BPTreeNode{Data: t.Data}
		node.Keys = []ReferencedValue{key}
		node.leafPointers = []MemoryPointer{value}
		buf, err := node.MarshalBinary()
		if err != nil {
			return err
		}
		offset, err := t.tree.NewPage(buf)
		if err != nil {
			return err
		}
		return t.meta.SetRoot(MemoryPointer{Offset: uint64(offset), Length: uint32(len(buf))})
	}

	path, err := t.traverse(key.Value, root, rootOffset)
	if err != nil {
		return err
	}

	// insert the key into the leaf
	n := path[0].node
	j, _ := slices.BinarySearchFunc(n.Keys, key, func(e ReferencedValue, t ReferencedValue) int {
		return bytes.Compare(e.Value, t.Value)
	})
	if j == len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.leafPointers = append(n.leafPointers, value)
	} else {
		n.Keys = append(n.Keys[:j+1], n.Keys[j:]...)
		n.Keys[j] = key
		n.leafPointers = append(n.leafPointers[:j+1], n.leafPointers[j:]...)
		n.leafPointers[j] = value
	}

	// traverse up the tree and split if necessary
	for i := 0; i < len(path); i++ {
		tr := path[i]
		n := tr.node
		if int(n.Size()) > t.tree.PageSize() {
			// split the node
			// mid is the key that will be inserted into the parent
			mid := len(n.Keys) / 2
			midKey := n.Keys[mid]

			// n is the left node, m the right node
			m := &BPTreeNode{Data: t.Data}
			if n.leaf() {
				m.leafPointers = n.leafPointers[mid:]
				m.Keys = n.Keys[mid:]
			} else {
				// for non-leaf nodes, the mid key is inserted into the parent
				m.internalPointers = n.internalPointers[mid+1:]
				m.Keys = n.Keys[mid+1:]
			}
			mbuf, err := m.MarshalBinary()
			if err != nil {
				return err
			}
			moffset, err := t.tree.NewPage(mbuf)
			if err != nil {
				return err
			}

			if n.leaf() {
				n.leafPointers = n.leafPointers[:mid]
				n.Keys = n.Keys[:mid]
			} else {
				n.internalPointers = n.internalPointers[:mid+1]
				n.Keys = n.Keys[:mid]
			}

			nbuf, err := n.MarshalBinary()
			if err != nil {
				return err
			}
			noffset := tr.ptr.Offset
			if _, err := t.tree.Seek(int64(noffset), io.SeekStart); err != nil {
				return err
			}
			if _, err := t.tree.Write(nbuf); err != nil {
				return err
			}

			// update the parent
			if i < len(path)-1 {
				p := path[i+1]
				// insert the key into the parent
				if p.index == len(p.node.Keys) {
					p.node.Keys = append(p.node.Keys, midKey)
				} else {
					p.node.Keys = append(p.node.Keys[:p.index+1], p.node.Keys[p.index:]...)
					p.node.Keys[p.index] = midKey
				}
				p.node.internalPointers = append(p.node.internalPointers[:p.index+1], p.node.internalPointers[p.index:]...)
				p.node.internalPointers[p.index] = uint64(noffset)
				p.node.internalPointers[p.index+1] = uint64(moffset)
				// the parent will be written to disk in the next iteration
			} else {
				// the root split, so create a new root
				p := &BPTreeNode{Data: t.Data}
				p.Keys = []ReferencedValue{midKey}
				p.internalPointers = []uint64{
					uint64(noffset), uint64(moffset),
				}

				pbuf, err := p.MarshalBinary()
				if err != nil {
					return err
				}
				poffset, err := t.tree.NewPage(pbuf)
				if err != nil {
					return err
				}
				if err := t.meta.SetRoot(MemoryPointer{Offset: uint64(poffset), Length: uint32(len(pbuf))}); err != nil {
					return err
				}
				return nil
			}
		} else {
			// write this node to disk and update the parent
			buf, err := tr.node.MarshalBinary()
			if err != nil {
				return err
			}
			if _, err := t.tree.Seek(int64(tr.ptr.Offset), io.SeekStart); err != nil {
				return err
			}
			if _, err := t.tree.Write(buf); err != nil {
				return err
			}
			// no new nodes were produced, so we can return here
			return nil
		}
	}
	panic("unreachable")
}

type Entry struct {
	Key   []byte
	Value MemoryPointer
}

// BulkInsert allows for the initial bulk loading of the tree. It is more efficient
// than inserting one key at a time because it does not traverse the tree for each
// key. Note that tree must be empty when calling this function.
// func (t *BPTree) BulkInsert(entries []Entry) error {
// 	// verify that the tree is empty
// 	if r, _, err := t.root(); err != nil || r != nil {
// 		return fmt.Errorf("tree is not empty, err: %w", err)
// 	}

// 	// sort the data entries by key
// 	slices.SortFunc(entries, func(x, y Entry) int {
// 		return bytes.Compare(x.Key, y.Key)
// 	})

// 	parents := []struct {
// 		key     []byte
// 		pointer uint64
// 	}{}

// 	offset, err := t.tree.Seek(0, io.SeekEnd)
// 	if err != nil {
// 		return err
// 	}

// 	// break into maxPageSize chunks
// 	for i := 0; i < len(entries); i += t.maxPageSize {
// 		chunk := entries[i:min(i+t.maxPageSize, len(entries))]

// 		// write the chunk to disk
// 		node := &BPTreeNode{}
// 		node.Keys = make([][]byte, len(chunk))
// 		node.Pointers = make([]MemoryPointer, len(chunk))
// 		for i, e := range chunk {
// 			node.Keys[i] = e.Key
// 			node.Pointers[i] = e.Value
// 		}
// 		n, err := node.WriteTo(t.tree)
// 		if err != nil {
// 			return err
// 		}
// 		parents = append(parents, struct {
// 			key     []byte
// 			pointer uint64
// 		}{key: chunk[0].Key, pointer: uint64(offset)})
// 		offset += n
// 	}

// 	for {
// 		nextParents := []struct {
// 			key     []byte
// 			pointer uint64
// 		}{}
// 		// break into maxPageSize chunks
// 		for i := 0; i < len(parents); i += t.maxPageSize {
// 			chunk := parents[i:min(i+t.maxPageSize, len(parents))]

// 			// write the chunk to disk
// 			node := &BPTreeNode{}
// 			node.Keys = make([][]byte, len(chunk)-1)
// 			node.Pointers = make([]MemoryPointer, len(chunk))
// 			for j, e := range chunk {
// 				if j > 0 {
// 					node.Keys[j-1] = e.key
// 				}
// 				node.Pointers[j] = e.pointer
// 			}
// 			n, err := node.WriteTo(t.tree)
// 			if err != nil {
// 				return err
// 			}
// 			nextParents = append(nextParents, struct {
// 				key     []byte
// 				pointer uint64
// 			}{key: chunk[0].key, pointer: uint64(offset)})
// 			offset += n
// 		}
// 		parents = nextParents
// 		if len(parents) == 1 {
// 			// this is the root
// 			return t.meta.SetRoot(parents[0].pointer)
// 		}
// 	}
// }

func (t *BPTree) recursiveString(n *BPTreeNode, indent int) string {
	// print the node itself
	var buf bytes.Buffer
	if !n.leaf() {
		for i := range n.internalPointers {
			child, err := t.readNode(n.Pointer(i))
			if err != nil {
				return fmt.Sprintf("error: failed to read child node: %v", err)
			}
			buf.WriteString(t.recursiveString(child, indent+1))
			if i < len(n.internalPointers)-1 {
				for i := 0; i < indent; i++ {
					buf.WriteString("  ")
				}
				buf.WriteString(fmt.Sprintf("key %v\n", n.Keys[i]))
			}
		}
	} else {
		for i := range n.leafPointers {
			for i := 0; i < indent; i++ {
				buf.WriteString("  ")
			}
			buf.WriteString(fmt.Sprintf("%v\n", n.Keys[i]))
		}
	}
	return buf.String()
}

func (t *BPTree) String() string {
	root, _, err := t.root()
	if err != nil {
		return fmt.Sprintf("error: failed to read root node: %v", err)
	}
	if root == nil {
		return "empty tree"
	}
	return "b+ tree ---\n" + t.recursiveString(root, 0)
}
