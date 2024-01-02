package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
)

type ReadWriteSeekTruncater interface {
	io.ReadWriteSeeker
	Truncate(size int64) error
}

type BPTree struct {
	tree ReadWriteSeekTruncater

	maxPageSize int
}

func NewBPTree(tree ReadWriteSeekTruncater, maxPageSize int) (*BPTree, error) {
	// read the root from the meta page
	var root uint64
	if err := binary.Read(tree, binary.BigEndian, &root); err != nil {
		if err == io.EOF {
			// empty tree
			if _, err := tree.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
			if _, err := tree.Write(make([]byte, 12)); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &BPTree{tree: tree, maxPageSize: maxPageSize}, nil
}

func (t *BPTree) root() (*BPTreeNode, MemoryPointer, error) {
	mp := MemoryPointer{}
	if _, err := t.tree.Seek(0, io.SeekStart); err != nil {
		return nil, mp, err
	}
	if err := binary.Read(t.tree, binary.BigEndian, &mp.Offset); err != nil {
		return nil, mp, err
	}
	if err := binary.Read(t.tree, binary.BigEndian, &mp.Length); err != nil {
		return nil, mp, err
	}
	if mp.Offset == 0 || mp.Length == 0 {
		return nil, mp, nil
	}
	root, err := t.readNode(mp)
	if err != nil {
		return nil, mp, err
	}
	return root, mp, nil
}

func (t *BPTree) writeRoot(mp MemoryPointer) error {
	if _, err := t.tree.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(t.tree, binary.BigEndian, mp.Offset); err != nil {
		return err
	}
	return binary.Write(t.tree, binary.BigEndian, mp.Length)
}

func (t *BPTree) Find(key []byte) (MemoryPointer, bool, error) {
	root, _, err := t.root()
	if err != nil {
		return MemoryPointer{}, false, fmt.Errorf("read root node: %w", err)
	}
	if root == nil {
		return MemoryPointer{}, false, nil
	}
	path, err := t.traverse(key, root)
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
	node := &BPTreeNode{}
	if _, err := node.ReadFrom(t.tree); err != nil {
		return nil, err
	}
	return node, nil
}

type TraversalRecord struct {
	node  *BPTreeNode
	index int
}

// traverse returns the path from root to leaf in reverse order (leaf first)
// the last element is always the node passed in
func (t *BPTree) traverse(key []byte, node *BPTreeNode) ([]*TraversalRecord, error) {
	if node.leaf() {
		return []*TraversalRecord{{node: node}}, nil
	}
	for i, k := range node.Keys {
		if bytes.Compare(key, k) < 0 {
			child, err := t.readNode(node.Pointers[i])
			if err != nil {
				return nil, err
			}
			path, err := t.traverse(key, child)
			if err != nil {
				return nil, err
			}
			return append(path, &TraversalRecord{node: node, index: i}), nil
		}
	}
	child, err := t.readNode(node.Pointers[len(node.Pointers)-1])
	if err != nil {
		return nil, err
	}
	path, err := t.traverse(key, child)
	if err != nil {
		return nil, err
	}
	return append(path, &TraversalRecord{node: node, index: len(node.Keys)}), nil
}

func (t *BPTree) Insert(key []byte, value MemoryPointer) error {
	root, rootOffset, err := t.root()
	if err != nil {
		return fmt.Errorf("read root node: %w", err)
	}
	if root == nil {
		// special case, create the root as the first node
		offset, err := t.tree.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		node := &BPTreeNode{}
		node.Keys = [][]byte{key}
		node.Pointers = []MemoryPointer{value}
		length, err := node.WriteTo(t.tree)
		if err != nil {
			return err
		}
		return t.writeRoot(MemoryPointer{Offset: uint64(offset), Length: uint32(length)})
	}
	path, err := t.traverse(key, root)
	if err != nil {
		return err
	}

	// insert the key into the leaf
	n := path[0].node
	j, _ := n.bsearch(key)
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
		if len(n.Keys) > t.maxPageSize {
			// split the node
			moffset, err := t.tree.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}

			// mid is the key that will be inserted into the parent
			mid := len(n.Keys) / 2
			midKey := n.Keys[mid]

			// n is the left node, m the right node
			m := &BPTreeNode{}
			if n.leaf() {
				m.Pointers = n.Pointers[mid:]
				m.Keys = n.Keys[mid:]
			} else {
				// for non-leaf nodes, the mid key is inserted into the parent
				m.Pointers = n.Pointers[mid+1:]
				m.Keys = n.Keys[mid+1:]
			}
			msize, err := m.WriteTo(t.tree)
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
			noffset := moffset + msize
			nsize, err := n.WriteTo(t.tree)
			if err != nil {
				return err
			}

			// update the parent
			if i < len(path)-1 {
				p := path[i+1]
				j, _ := p.node.bsearch(midKey)
				if j != p.index {
					// j should be equal to p.index...?
					// panic("aww")
				}
				// insert the key into the parent
				if j == len(p.node.Keys) {
					p.node.Keys = append(p.node.Keys, midKey)
				} else {
					p.node.Keys = append(p.node.Keys[:j+1], p.node.Keys[j:]...)
					p.node.Keys[j+1] = midKey
				}
				p.node.Pointers = append(p.node.Pointers[:j+1], p.node.Pointers[j:]...)
				p.node.Pointers[j] = MemoryPointer{Offset: uint64(noffset), Length: uint32(nsize)}
				p.node.Pointers[j+1] = MemoryPointer{Offset: uint64(moffset), Length: uint32(msize)}
				// the parent will be written to disk in the next iteration
			} else {
				poffset := noffset + nsize
				// create a new root
				p := &BPTreeNode{Pointers: []MemoryPointer{rootOffset}}
				p.Keys = [][]byte{m.Keys[0]}
				p.Pointers = []MemoryPointer{
					MemoryPointer{Offset: uint64(noffset), Length: uint32(nsize)},
					MemoryPointer{Offset: uint64(moffset), Length: uint32(msize)},
				}

				psize, err := p.WriteTo(t.tree)
				if err != nil {
					return err
				}
				return t.writeRoot(MemoryPointer{Offset: uint64(poffset), Length: uint32(psize)})
			}
		} else {
			// write this node to disk and update the parent
			offset, err := t.tree.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}
			length, err := tr.node.WriteTo(t.tree)
			if err != nil {
				return err
			}

			if i < len(path)-1 {
				p := path[i+1]
				// update the parent at the index
				p.node.Pointers[p.index] = MemoryPointer{Offset: uint64(offset), Length: uint32(length)}
			} else {
				// update the root
				return t.writeRoot(MemoryPointer{Offset: uint64(offset), Length: uint32(length)})
			}
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
// 			return t.writeRoot(parents[0].pointer)
// 		}
// 	}
// }

func (t *BPTree) compact() error {
	// read all the nodes and compile a list of nodes still referenced,
	// then write out the nodes in order, removing unreferenced nodes and updating
	// the parent pointers.

	_, rootOffset, err := t.root()
	if err != nil {
		return err
	}

	// skip the meta pointer
	if _, err := t.tree.Seek(12, io.SeekStart); err != nil {
		return err
	}

	references := []MemoryPointer{rootOffset}
	for {
		node := &BPTreeNode{}
		if _, err := node.ReadFrom(t.tree); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !node.leaf() {
			// all pointers are references
			references = append(references, node.Pointers...)
		}
	}

	// read all the nodes again and write out the referenced nodes
	if _, err := t.tree.Seek(12, io.SeekStart); err != nil {
		return err
	}

	slices.SortFunc(references, func(x, y MemoryPointer) int {
		return int(x.Offset - y.Offset)
	})

	referenceMap := make(map[uint64]MemoryPointer)

	offset := 12
	for i, reference := range references {
		// skip duplicates
		if i > 0 && references[i-1] == reference {
			continue
		}
		// read the referenced node
		if _, err := t.tree.Seek(int64(reference.Offset), io.SeekStart); err != nil {
			return err
		}
		node := &BPTreeNode{}
		if _, err := node.ReadFrom(t.tree); err != nil {
			return err
		}
		// write the node to the new offset
		if _, err := t.tree.Seek(int64(offset), io.SeekStart); err != nil {
			return err
		}
		n, err := node.WriteTo(t.tree)
		if err != nil {
			return err
		}
		// update the reference map
		referenceMap[reference.Offset] = MemoryPointer{Offset: uint64(offset), Length: uint32(n)}
		offset += int(n)
	}

	// truncate the file
	if err := t.tree.Truncate(int64(offset)); err != nil {
		return err
	}

	// update the parent pointers
	if _, err := t.tree.Seek(12, io.SeekStart); err != nil {
		return err
	}
	for {
		offset, err := t.tree.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		node := &BPTreeNode{}
		if _, err := node.ReadFrom(t.tree); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !node.leaf() {
			// all pointers are references
			for i, p := range node.Pointers {
				node.Pointers[i] = referenceMap[p.Offset]
			}
		}
		if _, err := t.tree.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		if _, err := node.WriteTo(t.tree); err != nil {
			return err
		}
	}

	// update the meta pointer
	return t.writeRoot(referenceMap[rootOffset.Offset])
}

func (t *BPTree) String() string {
	var buf bytes.Buffer
	// get the current seek position
	seekPos, err := t.tree.Seek(0, io.SeekCurrent)
	if err != nil {
		return err.Error()
	}
	defer func() {
		// reset the seek position
		if _, err := t.tree.Seek(seekPos, io.SeekStart); err != nil {
			panic(err)
		}
	}()
	root, rootOffset, err := t.root()
	if err != nil {
		return err.Error()
	}
	if root == nil {
		return "empty tree"
	}
	if _, err := buf.Write([]byte(fmt.Sprintf("root: %d\n", rootOffset))); err != nil {
		return err.Error()
	}
	// seek to 8
	if _, err := t.tree.Seek(12, io.SeekStart); err != nil {
		return err.Error()
	}
	for {
		offset, err := t.tree.Seek(0, io.SeekCurrent)
		if err != nil {
			return err.Error()
		}
		node := &BPTreeNode{}
		if _, err := node.ReadFrom(t.tree); err != nil {
			if err == io.EOF {
				break
			}
			return err.Error()
		}
		if node.leaf() {
			if _, err := buf.Write([]byte(fmt.Sprintf("%04d | ", offset))); err != nil {
				return err.Error()
			}
		} else {
			if _, err := buf.Write([]byte(fmt.Sprintf("%04d   ", offset))); err != nil {
				return err.Error()
			}
		}
		for _, p := range node.Pointers {
			if _, err := buf.Write([]byte(fmt.Sprintf("%04d ", p))); err != nil {
				return err.Error()
			}
		}
		if _, err := buf.Write([]byte("\n")); err != nil {
			return err.Error()
		}
	}
	return buf.String()
}
