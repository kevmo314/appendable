package btree

import (
	"bytes"
	"fmt"
	"io"
	"log"
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

	Data io.ReaderAt
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

func (t *BPTree) Find(key []byte) (MemoryPointer, bool, error) {
	root, rootOffset, err := t.root()
	if err != nil {
		return MemoryPointer{}, false, fmt.Errorf("read root node: %w", err)
	}
	if root == nil {
		return MemoryPointer{}, false, nil
	}
	path, err := t.traverse(key, root, rootOffset)
	if err != nil {
		return MemoryPointer{}, false, err
	}
	n := path[0].node
	i, found := n.bsearch(key)
	if found {
		return n.Pointers[i], true, nil
	}
	return MemoryPointer{}, false, nil
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

type TraversalRecord struct {
	node  *BPTreeNode
	index int
	// the offset is useful so we know which page to free when we split
	ptr MemoryPointer
}

// traverse returns the path from root to leaf in reverse order (leaf first)
// the last element is always the node passed in
func (t *BPTree) traverse(key []byte, node *BPTreeNode, ptr MemoryPointer) ([]TraversalRecord, error) {
	if node.leaf() {
		return []TraversalRecord{{node: node, ptr: ptr}}, nil
	}
	for i, k := range node.Keys {
		if bytes.Compare(key, k.Value) < 0 {
			if node.Pointers[i].Offset == ptr.Offset {
				log.Printf("infinite loop index %d", i)
				log.Printf("%#v", node)
				log.Printf("node offset %#v ptr offset %#v", node.Pointers[i].Offset, ptr.Offset)
				panic("infinite loop")
			}
			child, err := t.readNode(node.Pointers[i])
			if err != nil {
				return nil, err
			}
			path, err := t.traverse(key, child, node.Pointers[i])
			if err != nil {
				return nil, err
			}
			return append(path, TraversalRecord{node: node, index: i, ptr: ptr}), nil
		}
	}
	if node.Pointers[len(node.Pointers)-1].Offset == ptr.Offset {
		panic("infinite loop 2")
	}
	child, err := t.readNode(node.Pointers[len(node.Pointers)-1])
	if err != nil {
		return nil, err
	}
	path, err := t.traverse(key, child, node.Pointers[len(node.Pointers)-1])
	if err != nil {
		return nil, err
	}
	return append(path, TraversalRecord{node: node, index: len(node.Keys), ptr: ptr}), nil
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
		node.Pointers = []MemoryPointer{value}
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
	j, _ := n.bsearch(key.Value)
	if j == len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.Pointers = append(n.Pointers, value)
	} else {
		n.Keys = append(n.Keys[:j+1], n.Keys[j:]...)
		n.Keys[j] = key
		n.Pointers = append(n.Pointers[:j+1], n.Pointers[j:]...)
		n.Pointers[j] = value
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
				m.Pointers = n.Pointers[mid:]
				m.Keys = n.Keys[mid:]
			} else {
				// for non-leaf nodes, the mid key is inserted into the parent
				m.Pointers = n.Pointers[mid+1:]
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
				n.Pointers = n.Pointers[:mid]
				n.Keys = n.Keys[:mid]
			} else {
				n.Pointers = n.Pointers[:mid+1]
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
				p.node.Pointers = append(p.node.Pointers[:p.index+1], p.node.Pointers[p.index:]...)
				p.node.Pointers[p.index] = MemoryPointer{Offset: uint64(noffset), Length: uint32(len(nbuf))}
				p.node.Pointers[p.index+1] = MemoryPointer{Offset: uint64(moffset), Length: uint32(len(mbuf))}
				// the parent will be written to disk in the next iteration
			} else {
				// the root split, so create a new root
				p := &BPTreeNode{Data: t.Data}
				p.Keys = []ReferencedValue{midKey}
				p.Pointers = []MemoryPointer{
					{Offset: uint64(noffset), Length: uint32(len(nbuf))},
					{Offset: uint64(moffset), Length: uint32(len(mbuf))},
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
		for i := range n.Pointers {
			child, err := t.readNode(n.Pointers[i])
			if err != nil {
				return fmt.Sprintf("error: failed to read child node: %v", err)
			}
			buf.WriteString(t.recursiveString(child, indent+1))
			if i < len(n.Pointers)-1 {
				for i := 0; i < indent; i++ {
					buf.WriteString("  ")
				}
				buf.WriteString(fmt.Sprintf("key %v\n", n.Keys[i]))
			}
		}
	} else {
		for i := range n.Pointers {
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
