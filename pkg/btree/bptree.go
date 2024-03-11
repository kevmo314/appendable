package btree

import (
	"bytes"
	"fmt"
	"io"
	"slices"

	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

// MetaPage is an abstract interface over the root page of a btree
// This allows the caller to control the memory location of the meta
// pointer
type MetaPage interface {
	Root() (MemoryPointer, error)
	SetRoot(MemoryPointer) error
}

type BPTree struct {
	PageFile pagefile.ReadWriteSeekPager
	MetaPage MetaPage

	Data       []byte
	DataParser DataParser

	Width uint16
}

func (t *BPTree) root() (*BPTreeNode, MemoryPointer, error) {
	mp, err := t.MetaPage.Root()
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

type TraversalIterator struct {
	tree    *BPTree
	key     ReferencedValue
	records []TraversalRecord
	err     error
}

func (p *TraversalIterator) Key() ReferencedValue {
	return p.records[0].node.Keys[p.records[0].index]
}

func (p *TraversalIterator) Pointer() MemoryPointer {
	return p.records[0].node.Pointer(p.records[0].index)
}

func (p *TraversalIterator) init() bool {
	root, rootOffset, err := p.tree.root()
	if err != nil {
		p.err = fmt.Errorf("read root node: %w", err)
		return false
	}
	if root == nil {
		return false
	}

	path, err := p.tree.traverse(p.key, root, rootOffset)

	if err != nil {
		p.err = err
		return false
	}
	p.records = path
	return true
}

// incr moves the iterator by delta, returning false if there is no more data
// delta is taken to be either -1 or 1.
func (p *TraversalIterator) incr(i, delta int) bool {
	if i == len(p.records) {
		// we can't increment beyond the root
		return false
	}
	p.records[i].index += delta
	rolloverLeft := p.records[i].index < 0
	rolloverRight := p.records[i].index >= p.records[i].node.NumPointers()
	if rolloverLeft || rolloverRight {
		// increment the parent node
		if !p.incr(i+1, delta) {
			// if we weren't able to, return false
			return false
		}
		// otherwise, update the current node
		node, err := p.tree.readNode(p.records[i+1].node.Pointer(p.records[i+1].index))
		if err != nil {
			p.err = err
			return false
		}
		// then propagate the rollover
		p.records[i].node = node
		if rolloverLeft {
			p.records[i].index = p.records[i].node.NumPointers() - 1
		} else {
			p.records[i].index = 0
		}
	}
	return true
}

func (p *TraversalIterator) Next() bool {
	if p.records == nil {
		return p.init()
	}
	return p.incr(0, 1)
}

func (p *TraversalIterator) Prev() bool {
	if p.records == nil && !p.init() {
		return false
	}
	return p.incr(0, -1)
}

func (p *TraversalIterator) Err() error {
	return p.err
}

func (t *BPTree) Iter(key ReferencedValue) (*TraversalIterator, error) {
	return &TraversalIterator{tree: t, key: key}, nil
}

func (t *BPTree) Find(key ReferencedValue) (ReferencedValue, MemoryPointer, error) {
	p, err := t.Iter(key)
	if err != nil {
		return ReferencedValue{}, MemoryPointer{}, err
	}
	if !p.Next() {
		return ReferencedValue{}, MemoryPointer{}, p.Err()
	}
	return p.Key(), p.Pointer(), nil
}

func (t *BPTree) readNode(ptr MemoryPointer) (*BPTreeNode, error) {
	if _, err := t.PageFile.Seek(int64(ptr.Offset), io.SeekStart); err != nil {
		return nil, err
	}
	node := &BPTreeNode{Data: t.Data, DataParser: t.DataParser, Width: t.Width}
	buf := make([]byte, t.PageFile.PageSize())
	if _, err := t.PageFile.Read(buf); err != nil {
		return nil, err
	}
	if err := node.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return node, nil
}

func (t *BPTree) first() (ReferencedValue, error) {
	rootNode, _, err := t.root()

	if err != nil {
		return ReferencedValue{}, err
	}

	currNode, err := t.readNode(rootNode.Pointer(0))
	if err != nil {
		return ReferencedValue{}, err
	}

	for !currNode.Leaf() {
		childPointer := currNode.Pointer(0)
		currNode, err = t.readNode(childPointer)

		if err != nil {
			return ReferencedValue{}, err
		}
	}

	return currNode.Keys[0], nil
}

func (t *BPTree) last() (ReferencedValue, error) {
	rootNode, _, err := t.root()

	if err != nil {
		return ReferencedValue{}, err
	}

	currNode, err := t.readNode(rootNode.Pointer(rootNode.NumPointers() - 1))
	if err != nil {
		return ReferencedValue{}, err
	}

	for !currNode.Leaf() {
		childPointer := currNode.Pointer(currNode.NumPointers() - 1)
		currNode, err = t.readNode(childPointer)

		if err != nil {
			return ReferencedValue{}, err
		}
	}

	return currNode.Keys[0], nil
}

// traverse returns the path from root to leaf in reverse order (leaf first)
// the last element is always the node passed in
func (t *BPTree) traverse(key ReferencedValue, node *BPTreeNode, ptr MemoryPointer) ([]TraversalRecord, error) {
	// binary search node.Keys to find the first key greater than key
	index, found := slices.BinarySearchFunc(node.Keys, key, CompareReferencedValues)

	if node.Leaf() {
		return []TraversalRecord{{node: node, index: index, ptr: ptr}}, nil
	}

	if found {
		// if the key is found, we need to go to the right child
		index++
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
		node := &BPTreeNode{Data: t.Data, DataParser: t.DataParser, Width: t.Width}
		node.Keys = []ReferencedValue{key}
		node.leafPointers = []MemoryPointer{value}
		buf, err := node.MarshalBinary()
		if err != nil {
			return err
		}
		offset, err := t.PageFile.NewPage(buf)
		if err != nil {
			return err
		}
		return t.MetaPage.SetRoot(MemoryPointer{Offset: uint64(offset), Length: encoding.EncodeFloatingInt16(len(buf))})
	}

	path, err := t.traverse(key, root, rootOffset)
	if err != nil {
		return err
	}

	// insert the key into the leaf
	n := path[0].node
	j, found := slices.BinarySearchFunc(n.Keys, key, CompareReferencedValues)
	if found {
		return fmt.Errorf("key already exists")
	}
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
		if int(n.Size()) > t.PageFile.PageSize() {
			// split the node
			// mid is the key that will be inserted into the parent
			mid := len(n.Keys) / 2
			midKey := n.Keys[mid]

			// n is the left node, m the right node
			m := &BPTreeNode{Data: t.Data, DataParser: t.DataParser, Width: t.Width}
			if n.Leaf() {
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
			moffset, err := t.PageFile.NewPage(mbuf)
			if err != nil {
				return err
			}

			if n.Leaf() {
				n.leafPointers = n.leafPointers[:mid]
				n.Keys = n.Keys[:mid]
			} else {
				n.internalPointers = n.internalPointers[:mid+1]
				n.Keys = n.Keys[:mid]
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
				p := &BPTreeNode{Data: t.Data, DataParser: t.DataParser, Width: t.Width}
				p.Keys = []ReferencedValue{midKey}
				p.internalPointers = []uint64{
					uint64(noffset), uint64(moffset),
				}

				pbuf, err := p.MarshalBinary()
				if err != nil {
					return err
				}
				poffset, err := t.PageFile.NewPage(pbuf)
				if err != nil {
					return err
				}
				if err := t.MetaPage.SetRoot(MemoryPointer{Offset: uint64(poffset), Length: encoding.EncodeFloatingInt16(len(pbuf))}); err != nil {
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
	if !n.Leaf() {
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
