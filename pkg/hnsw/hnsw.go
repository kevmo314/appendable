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

func (h *Hnsw) spawnLayer() int {
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

func (h *Hnsw) searchLayer(q Vector, entryNode *Node, ef int, layerId int) (*BaseQueue, error) {

	// visited is a bitset that keeps track of all nodes that have been visited.
	// we know the size of visited will never exceed len(h.Nodes)
	visited := make([]bool, len(h.Nodes))

	if entryNode.id != h.EntryNodeId {
		panic(fmt.Sprintf("debug: this should not occur. entry node mismatch got %v, expected: %v", entryNode.id, h.EntryNodeId))
	}

	visited[entryNode.id] = true

	entryNodeToQDist := EuclidDist(entryNode.v, q)

	candidates := NewBaseQueue(MinComparator{})
	candidates.Insert(entryNode.id, entryNodeToQDist)

	nearestNeighborsToQForEf := NewBaseQueue(MaxComparator{})
	nearestNeighborsToQForEf.Insert(entryNode.id, entryNodeToQDist)

	for !candidates.IsEmpty() {
		// extract nearest element from C to q
		closestCandidate, err := candidates.Peel()
		if err != nil {
			return nil, err
		}

		// get the furthest element from W to q
		furthestNN, err := nearestNeighborsToQForEf.Peel()
		if err != nil {
			return nil, err
		}

		closestCandidateToQDist := EuclidDist(h.Nodes[closestCandidate.id].v, q)
		furthestNNToQDist := EuclidDist(h.Nodes[furthestNN.id].v, q)

		if closestCandidateToQDist > furthestNNToQDist {
			// all elements in W are evaluated
			break
		}

		if len(h.Nodes[closestCandidate.id].friends) >= layerId+1 {
			friends := h.Nodes[closestCandidate.id].friends[layerId]

			for !friends.IsEmpty() {
				friend, err := friends.Peel()
				if err != nil {
					return nil, err
				}
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

	numNearestToQ, err := nearestNeighborsToQForEf.Take(ef, MinComparator{})
	if err != nil {
		return nil, err
	}

	return numNearestToQ, nil
}

func (h *Hnsw) selectNeighbors(candidates *BaseQueue, numNeighborsToReturn int) (*BaseQueue, error) {
	if candidates.Len() <= numNeighborsToReturn {
		return nil, fmt.Errorf("num neighbors to return is %v but candidates len is only %v", numNeighborsToReturn, candidates.Len())
	}

	pq, err := candidates.Take(numNeighborsToReturn, MinComparator{})
	if err != nil {
		return nil, fmt.Errorf("an error occured during take: %v", err)
	}

	return pq, nil
}

func (h *Hnsw) KnnSearch(q Vector, kNeighborsToReturn, ef int) ([]*Item, error) {
	currentNearestElements := NewBaseQueue(MinComparator{})
	entryPointNode := h.Nodes[h.EntryNodeId]

	for l := entryPointNode.layer; l >= 1; l-- {
		numNearestToQAtLevelL, err := h.searchLayer(q, entryPointNode, 1, l)

		if err != nil {
			return nil, err
		}

		for !numNearestToQAtLevelL.IsEmpty() {
			peeled, err := numNearestToQAtLevelL.Peel()

			if err != nil {
				return nil, err
			}

			currentNearestElements.Insert(peeled.id, peeled.dist)
		}

		nearest, err := currentNearestElements.Peel()

		if err != nil {
			return nil, err
		}

		entryPointNode = h.Nodes[nearest.id]
	}

	numNearestToQAtBase, err := h.searchLayer(q, entryPointNode, ef, 0)

	if err != nil {
		return nil, err
	}

	for !numNearestToQAtBase.IsEmpty() {
		peeled, err := numNearestToQAtBase.Peel()
		if err != nil {
			return nil, err
		}
		currentNearestElements.Insert(peeled.id, peeled.dist)
	}

	if currentNearestElements.Len() < kNeighborsToReturn {
		panic("")
	}

	pq, err := currentNearestElements.Take(kNeighborsToReturn, MinComparator{})
	if err != nil {
		return nil, fmt.Errorf("failed to knnsearch, err: %v", err)
	}

	return pq.items, nil
}

func (h *Hnsw) Link(i0, i1 *Item, level int) {
	n0, n1 := h.Nodes[i0.id], h.Nodes[i1.id]
	f0, f1 := n0.friends, n1.friends

	mq0, mq1 := f0[level], f1[level]

	mq0.Insert(i1.id, i1.dist)
	mq1.Insert(i0.id, i0.dist)
}

func (h *Hnsw) Insert(q Vector) error {

	// 1. build Node for vec q
	qLayer := h.spawnLayer()
	qNode := NewNode(h.NextNodeId, q, qLayer)

	h.NextNodeId++
	qItem := Item{
		id:   qNode.id,
		dist: EuclidDist(qNode.v, h.Nodes[h.EntryNodeId].v),
	}

	// 2. from top -> qlayer, make the first pass
	ep := h.Nodes[h.EntryNodeId]
	currentTopLayer := ep.layer

	// start at the top
	for level := currentTopLayer; level > qLayer; level-- {
		nnToQAtLevel, err := h.searchLayer(q, ep, 1, level)

		if err != nil {
			return err
		}

		if nnToQAtLevel.IsEmpty() {
			return fmt.Errorf("no nearest neighbors to q at level %v", level)
		}

		nearest, err := nnToQAtLevel.Peel()

		if err != nil {
			return err
		}

		// at each level, find the nearest neighbor to Q at that given level,
		// set the entryPointNode for the next iter
		ep = h.Nodes[nearest.id]
	}

	// 3. make the second pass, this time create connections
	for level := min(currentTopLayer, qLayer); level >= 0; level-- {
		nnToQAtLevel, err := h.searchLayer(q, ep, h.EfConstruction, level)
		if err != nil {
			return err
		}

		neighbors, err := h.selectNeighbors(nnToQAtLevel, h.M)

		if err != nil {
			return err
		}

		for !neighbors.IsEmpty() {
			peeled, err := neighbors.Peel()
			if err != nil {
				return err
			}
			qNode.friends[level].Insert(peeled.id, peeled.dist)
		}
	}

	// 4. add qNode into the `Nodes` table
	h.Nodes[qNode.id] = qNode

	// 5. Link connections
	for level := min(currentTopLayer, qLayer); level >= 0; level-- {
		friendsAtLevel := qNode.friends[level]

		for !friendsAtLevel.IsEmpty() {
			qfriends, err := friendsAtLevel.Peel()
			if err != nil {
				return err
			}
			h.Link(qfriends, &qItem, level)

			qFriendNode := h.Nodes[qfriends.id]
			qFriendNodeFriendsAtLevel := qFriendNode.friends[level]
			numFriendsForQFriendAtLevel := qFriendNodeFriendsAtLevel.Len()

			if (level == 0 && numFriendsForQFriendAtLevel > h.MMax0) || (level != 0 && numFriendsForQFriendAtLevel > h.MMax) {
				var amt int
				if level == 0 {
					amt = h.MMax0
				} else {
					amt = h.MMax
				}

				pq, err := qFriendNodeFriendsAtLevel.Take(amt, MinComparator{})
				if err != nil {
					return fmt.Errorf("failed to take friend id %v's %v at level %v", qfriends.id, amt, level)
				}

				// shrink connections for a friend at layer
				h.Nodes[qfriends.id].friends[level] = pq
			}
		}
	}

	// 6. update attr
	if h.MaxLayer < qLayer {
		h.MaxLayer = qLayer
		h.EntryNodeId = qNode.id
	}

	return nil
}
