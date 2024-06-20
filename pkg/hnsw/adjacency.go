package hnsw

import (
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

type AdjacencyPage [16][8]uint32

type Node struct {
	id     Id
	friend *Friends
	point  *Point
}

type AdjacencyPageManager struct {
	pointsManager  AdjacencyPointsManager
	friendsManager AdjacencyFriendsManager
}

type AdjacencySerializable interface {
	GetAdjacencies(x Id) [8]uint32
	AddAdjacency(x, y Id)
	RemoveAdjacency(x, y Id)
	AddNode(x *Node)
}

type AdjacencyPointsManager struct {
	tree   *btree.BTree
	points map[Id]*Point
}

type AdjacencyFriendsManager struct {
	tree    *btree.BTree
	friends map[Id]*Friends
}

func NewAdjacencyPointsManager(
	pf *pagefile.PageFile,
	points map[Id]*Point,
) *AdjacencyPointsManager {
	return &AdjacencyPointsManager{
		tree:   nil, // for now
		points: points,
	}
}

func NewAdjacencyFriendsManager(
	pf *pagefile.PageFile,
	friends map[Id]*Friends,
) *AdjacencyFriendsManager {
	return &AdjacencyFriendsManager{
		tree:    nil,
		friends: friends,
	}
}
