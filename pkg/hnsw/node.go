package hnsw

type NodeID = uint32
type Vector = []float32

type Node struct {
	// Index of the vector
	id NodeID
	v  Vector

	layer int

	// Layered list of neighbors,
	// each layer is a slice of NodeIds
	friends [][]NodeID
}

func NewNode(id NodeID, v Vector) *Node {
	return &Node{
		id,
		v,
		make([][]NodeID, 0),
	}
}
