package hnsw

import (
	"fmt"
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
	Nodes map[NodeId]*Node

	EntryNodeId NodeId
	NextNodeId  NodeId

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
	nt := make(map[NodeId]*Node)

	enId := NodeId(0) // special. Reserved for the entryPointNode

	nt[enId] = entryPoint

	nextId := enId + 1

	h := &Hnsw{
		vectorDimension: d,
		M:               m,
		Nodes:           nt,
		EntryNodeId:     enId,
		NextNodeId:      nextId,
		MaxLayer:        -1,
		levelMultiplier: 1 / math.Log(float64(m)),
		EfConstruction:  efc,
		MMax:            m,
		MMax0:           m * 2,
	}

	return h
}

func (h *Hnsw) entryTopLayer() int {
	return h.Nodes[h.EntryNodeId].layer
}
func (h *Hnsw) spawnLayer() int {
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

/*
searchLayer needs two things:
1. todo! an item from a euc queue that computes the distance from the entry point node -> q.
*/
func (h *Hnsw) searchLayer(q Vector, entryNode *Node, ef int, layerId int) *MinQueue {

	// visited is a bitset that keeps track of all nodes that have been visited.
	// we know the size of visited will never exceed len(h.Nodes)
	visited := make([]bool, len(h.Nodes))

	if entryNode.id != h.EntryNodeId {
		panic(fmt.Sprintf("debug: this should not occur. entry node mismatch got %v, expected: %v", entryNode.id, h.EntryNodeId))
	}

	visited[entryNode.id] = true

	entryNodeToQDist := EuclidDist(entryNode.v, q)

	candidates := NewMinQueue()
	candidates.Insert(entryNode.id, entryNodeToQDist)

	nearestNeighborsToQForEf := NewMaxQueue()
	nearestNeighborsToQForEf.Insert(entryNode.id, entryNodeToQDist)

	for !candidates.IsEmpty() {
		// extract nearest element from C to q
		closestCandidate := candidates.Peel()

		// get the furthest element from W to q
		furthestNN := nearestNeighborsToQForEf.Peel()

		closestCandidateToQDist := EuclidDist(h.Nodes[closestCandidate.id].v, q)
		furthestNNToQDist := EuclidDist(h.Nodes[furthestNN.id].v, q)

		if closestCandidateToQDist > furthestNNToQDist {
			// all elements in W are evaluated
			break
		}

		if len(h.Nodes[closestCandidate.id].friends) >= layerId+1 {
			friends := h.Nodes[closestCandidate.id].friends[layerId]

			for !friends.IsEmpty() {
				friend := friends.Peel()
				friendId := friend.id

				// if friendId ∉ visitor
				if !visited[friendId] {
					visited[friendId] = true
					furthestNNItem := nearestNeighborsToQForEf.Peek()

					friendToQDist := EuclidDist(h.Nodes[friendId].v, q)
					furthestNNToQDist := EuclidDist(h.Nodes[furthestNNItem.id].v, q)

					if friendToQDist < furthestNNToQDist || nearestNeighborsToQForEf.Len() < ef {
						candidates.Insert(friendId, friendToQDist)
						nearestNeighborsToQForEf.Insert(friendId, friendToQDist)

						if nearestNeighborsToQForEf.Len() > ef {
							nearestNeighborsToQForEf.Pop()
						}
					}
				}
			}
		}
	}

	numNearestToQ := NewMinQueue()
	for !nearestNeighborsToQForEf.IsEmpty() {
		peeled := nearestNeighborsToQForEf.Peel()
		numNearestToQ.Insert(peeled.id, peeled.dist)
	}

	return numNearestToQ
}

func (h *Hnsw) selectNeighbors(candidates *MinQueue, numNeighborsToReturn int) *MinQueue {

	if candidates.Len() <= numNeighborsToReturn {
		return nil
	}

	mCandidatesNearestElementsFromQ := NewMinQueue()

	for candidate := candidates.Peel(); candidate != nil; candidate = candidates.Peel() {
		if mCandidatesNearestElementsFromQ.Len() == numNeighborsToReturn {
			return mCandidatesNearestElementsFromQ
		}

		mCandidatesNearestElementsFromQ.Insert(candidate.id, candidate.dist)
	}

	return nil
}

func (h *Hnsw) KnnSearch(q Vector, kNeighborsToReturn, ef int) ([]*Item, error) {
	currentNearestElements := NewMinQueue()
	entryPointNode := h.Nodes[h.EntryNodeId]

	for l := entryPointNode.layer; l >= 1; l-- {
		numNearestToQAtLevelL := h.searchLayer(q, entryPointNode, 1, l)

		for !numNearestToQAtLevelL.IsEmpty() {
			peeled := numNearestToQAtLevelL.Peel()
			currentNearestElements.Insert(peeled.id, peeled.dist)
		}

		entryPointNode = h.Nodes[currentNearestElements.Peel().id]
	}

	numNearestToQAtBase := h.searchLayer(q, entryPointNode, ef, 0)

	for !numNearestToQAtBase.IsEmpty() {
		peeled := numNearestToQAtBase.Peel()
		currentNearestElements.Insert(peeled.id, peeled.dist)
	}

	if currentNearestElements.Len() < kNeighborsToReturn {
		panic("")
	}

	return currentNearestElements.Take(kNeighborsToReturn)
}

func (h *Hnsw) Link(i0, i1 *Item, level int) {
	n0, n1 := h.Nodes[i0.id], h.Nodes[i1.id]
	f0, f1 := n0.friends, n1.friends

	mq0, mq1 := f0[level], f1[level]

	mq0.Insert(i1.id, i1.dist)
	mq1.Insert(i0.id, i0.dist)
}
