package hnsw

import (
	"math"
	"math/rand"
)

/*
The greedy algorithm can be divided into two phases: zoom-out and zoom-in.
Starts in the zoom-out phase from a low degree node, traverses the graph increasing the node's degree.
Halts when characteristic radius of the node links length reaches the scale of the distance to the query.
*/

// Hnsw is a multilayer graph
type Hnsw struct {
	vectorDimension int

	// A lookup table for all nodes that exist in this graph
	Nodes map[NodeID]*Node

	EntryNodeID NodeID
	NextNodeID  NodeID

	MaxLayer int

	// default number of connections
	M int

	//  Maximum number of connections per element per layer
	MMax, MMax0 int

	// Size of dynamic candidate list during construction
	EfConstruction int

	// Normalization factor for level generation
	levelMultiplier float64
}

// New needs two things: vector dimensionality d
// and m the number of neighbors for each vertex
func NewHNSW(d, m int, efc int, entryPoint *Node) *Hnsw {
	nt := make(map[NodeID]*Node)

	enId := NodeID(0) // special. Reserved for the entryPointNode
	nt[enId] = entryPoint

	nextId := enId + 1

	h := &Hnsw{
		vectorDimension: d,
		M:               m,
		Nodes:           nt,
		EntryNodeID:     enId,
		NextNodeID:      nextId,
		MaxLayer:        -1,
		levelMultiplier: 1 / math.Log(float64(m)),
		EfConstruction:  efc,
		MMax:            m,
		MMax0:           m * 2,
	}

	return h
}

func (h *Hnsw) entryTopLayer() int {
	return h.Nodes[h.EntryNodeID].layer
}
func (h *Hnsw) spawnLayer() int {
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

/*
searchLayer needs two things:
1. todo! an item from a euc queue that computes the distance from the entry point node -> q.
2.
*/
func (h *Hnsw) searchLayer(q Vector, ef, layerId int, nearestNeighborsToQForEf *EucQueue) {

	// visited is a bitset that keeps track of all nodes that have been visited.
	// we know the size of visited will never exceed len(h.Nodes)
	visited := make([]bool, len(h.Nodes))
	visited[h.EntryNodeID] = true

	candidates := NewEucQueue(true)
	candidates.Push(h.EntryNodeID, 0) // todo fix! should be the dist from en -> q.

	nearestNeighborsToQForEf.Push(h.EntryNodeID, 0) // todo fix! ^^

	for !candidates.IsEmpty() {
		// extract nearest element from C to q
		closestCandidate := candidates.Pop()

		// get the furthest element from W to q
		furthestNN := nearestNeighborsToQForEf.Pop()

		closestCandidateToQDist := EuclidDist(h.Nodes[closestCandidate.id].v, q)
		furthestNNToQDist := EuclidDist(h.Nodes[furthestNN.id].v, q)

		if closestCandidateToQDist > furthestNNToQDist {
			// all elements in W are evaluated
			break
		}

		if len(h.Nodes[closestCandidate.id].friends) >= layerId+1 {
			friends := h.Nodes[closestCandidate.id].friends[layerId]

			for _, friendId := range friends {
				// if friendId ∉ visitor
				if !visited[friendId] {
					visited[friendId] = true
					furthestNNItem := nearestNeighborsToQForEf.Peek()

					friendToQDist := EuclidDist(h.Nodes[friendId].v, q)
					furthestNNToQDist := EuclidDist(h.Nodes[furthestNNItem.id].v, q)

					if friendToQDist < furthestNNToQDist || nearestNeighborsToQForEf.Len() < ef {
						candidates.Push(friendId, friendToQDist)
						nearestNeighborsToQForEf.Push(friendId, friendToQDist)

						if nearestNeighborsToQForEf.Len() > ef {
							nearestNeighborsToQForEf.Pop()
						}
					}
				}
			}
		}
	}
}
