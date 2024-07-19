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
	btree *btree.BTree
	// vectors []*hnsw.Point

	bptree *bptree.BPTree
	// neighborhood map[hnsw.Id]*hnsw.Friends

	hnsw *hnsw.Hnsw
}

func NewVectorPageManager(btree *btree.BTree, bptree *bptree.BPTree, hnsw *hnsw.Hnsw) *VectorPageManager {
	if btree == nil || bptree == nil {
		panic("btree and bptree must not be nil")
	}

	return &VectorPageManager{
		btree:  btree,
		bptree: bptree,
		hnsw:   hnsw,
	}
}

func (vp *VectorPageManager) AddNode(x hnsw.Point) error {
	xId, err := vp.hnsw.InsertVector(x)
	if err != nil {
		return err
	}

	// write point to btree
	if err := vp.btree.Insert(pointer.ReferencedId{Value: xId}, x); err != nil {
		return err
	}

	// write friends to bptree
	xFriends, err := vp.hnsw.Neighborhood(xId)
	if err != nil {
		return fmt.Errorf("vector id %v not found in hnsw neighborhood", x)
	}
	xfriendsBuf, err := xFriends.Flush(8)
	if err != nil {
		return err
	}

	if err := vp.bptree.Insert(pointer.ReferencedValue{Value: xfriendsBuf}, pointer.MemoryPointer{}); err != nil {
		return fmt.Errorf("failed to insert buf: %v", err)
	}

	return nil
}
