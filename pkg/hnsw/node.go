package hnsw

import (
	"fmt"
	"math"
)

type NodeID = uint32

type Vector []float32

func Eucdist(v0, v1 Vector) (float64, error) {
	if len(v0) != len(v1) {
		return -1, fmt.Errorf("vectors must be of the same dimension")
	}

	var sum float64

	for i := range v0 {
		delta := float64(v0[i] - v1[i])
		sum += delta * delta
	}

	return math.Sqrt(sum), nil
}

type Node struct {
	// Index of the vector
	id NodeID
	v  Vector

	layer int

	// for every layer, we have a list of friends' NodeIDs
	friends [][]NodeID
}

func NewNode(id NodeID, v Vector) *Node {
	return &Node{
		id,
		v,
		-1,
		make([][]NodeID, 0),
	}
}
