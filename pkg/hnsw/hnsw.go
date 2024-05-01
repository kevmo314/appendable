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
func NewHNSW(d, m int, efc int) *Hnsw {
	h := &Hnsw{
		vectorDimension: d,
		M:               m,
		Nodes:           make(map[NodeID]*Node),
		EntryNodeID:     ^uint32(0),
		MaxLayer:        -1,
		levelMultiplier: 1 / math.Log(float64(m)),
		EfConstruction:  efc,
		MMax:            m,
		MMax0:           m * 2,
	}

	return h
}

func (h *Hnsw) EntryTopLayer() int {
	return h.Nodes[h.EntryNodeID].layer
}
func (h *Hnsw) SpawnLayer() int {
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

// w must be a max euc queue
func (h *Hnsw) searchLayer(q Vector, ef, layerId int, w *EucQueue) {

	// visited is a bitset that keeps track of all nodes that have been visited.
	// we know the size of visited will never exceed len(h.Nodes)
	visited := make([]bool, len(h.Nodes))
	visited[h.EntryNodeID] = true

	candidates := NewEucQueue(true)
	candidates.Push(h.EntryNodeID, 0)

	w.Push(h.EntryNodeID, 0)

	for !candidates.IsEmpty() {
		// extract nearest element from C to q
		c := candidates.Pop()

		// get the furthest element from W to q
		f := w.Pop()

		cq := EuclidDist(h.Nodes[c.id].v, q)
		fq := EuclidDist(h.Nodes[f.id].v, q)

		if cq > fq {
			// all elements in W are evaluated
			break
		}

		if len(h.Nodes[c.id].friends) >= layerId+1 {
			friends := h.Nodes[c.id].friends[layerId]

			for _, friendId := range friends {
				// if e ∉ v
				if !visited[friendId] {
					visited[friendId] = true
					maxItem := w.Peek()

					eqDist := EuclidDist(h.Nodes[friendId].v, q)
					maxDist := EuclidDist(h.Nodes[maxItem.id].v, q)

					if eqDist < maxDist || w.Len() < ef {
						candidates.Push(friendId, eqDist)
						w.Push(friendId, eqDist)

						if w.Len() > ef {
							w.Pop()
						}
					}
				}
			}
		}
	}
}
