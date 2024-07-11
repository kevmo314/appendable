package vectorpage

import (
	"fmt"
	"github.com/kevmo314/appendable/pkg/bptree"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/hnsw"
	"github.com/kevmo314/appendable/pkg/pointer"
)

type HNSWAdjacencyPage [16][8]uint32

type VectorPageManager struct {
	btree   *btree.BTree
	vectors []*hnsw.Point

	bptree       *bptree.BPTree
	neighborhood map[hnsw.Id]*hnsw.Friends
}

func NewVectorPageManager(btree *btree.BTree, bptree *bptree.BPTree, vectors []*hnsw.Point, neighborhood map[hnsw.Id]*hnsw.Friends) *VectorPageManager {
	if btree == nil || bptree == nil {
		panic("btree and bptree must not be nil")
	}

	return &VectorPageManager{btree, vectors, bptree, neighborhood}
}

func (vp *VectorPageManager) AddNode(x hnsw.Id) error {
	// we'll assume that this node id is the freshly inserted vector
	xvector := *vp.vectors[x]

	if err := vp.btree.Insert(pointer.ReferencedId{Value: x}, xvector); err != nil {
		return err
	}

	xfriends, ok := vp.neighborhood[x]

	if !ok {
		return fmt.Errorf("vector id %v not found in hnsw neighborhood", x)
	}

	xfriendsBuf, err := xfriends.Flush(8)
	if err != nil {
		return err
	}

	if err := vp.bptree.Insert(pointer.ReferencedValue{Value: xfriendsBuf}, pointer.MemoryPointer{}); err != nil {
		return fmt.Errorf("failed to insert buf: %v", err)
	}

	return nil
}
