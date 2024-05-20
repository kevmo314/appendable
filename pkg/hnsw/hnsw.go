package hnsw

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
)

type Hnsw struct {
	Nodes map[NodeId]*Node

	EntryNodeId NodeId
	NextNodeId  NodeId

	MaxLevel uint

	// default number of connections
	M int

	//  Maximum number of connections per element per level
	MMax, MMax0 int

	// Size of dynamic candidate list during construction
	EfConstruction int

	// Normalization factor for level generation
	levelMultiplier float64
}

func NewHNSW(m int, efc int, entryPoint *Node) *Hnsw {
	nt := make(map[NodeId]*Node)
	enId := NodeId(0) // special. Reserved for the entryPointNode
	nt[enId] = entryPoint

	h := &Hnsw{
		M:               m,
		Nodes:           nt,
		EntryNodeId:     enId,
		NextNodeId:      enId + 1,
		MaxLevel:        entryPoint.level,
		levelMultiplier: 1 / math.Log(float64(m)),
		EfConstruction:  efc,
		MMax:            m,
		MMax0:           m * 2,
	}

	return h
}

func (h *Hnsw) getNextNodeId() NodeId {
	return atomic.AddUint32(&h.NextNodeId, 1) - 1
}

func (h *Hnsw) spawnLevel() uint {
	return uint(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

func (h *Hnsw) searchLevel(q Vector, entryNodeItem *Item, ef int, levelId uint) (*BaseQueue, error) {
	// visited is a bitset that keeps track of all nodes that have been visited.
	// we know the size of visited will never exceed len(h.Nodes)
	visited := make([]bool, len(h.Nodes))

	if entryNodeItem.id != h.EntryNodeId {
		panic(fmt.Sprintf("debug: this should not occur. entry node mismatch got %v, expected: %v", entryNodeItem.id, h.EntryNodeId))
	}

	visited[entryNodeItem.id] = true

	candidates := NewBaseQueue(MinComparator{})
	candidates.Insert(entryNodeItem.id, entryNodeItem.dist)

	nearestNeighborsToQForEf := NewBaseQueue(MaxComparator{})
	nearestNeighborsToQForEf.Insert(entryNodeItem.id, entryNodeItem.dist)

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

		friends := h.Nodes[closestCandidate.id].GetFriendsAtLevel(levelId)

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
	entryPointItem := &Item{id: h.EntryNodeId, dist: entryPointNode.VecDistFromVec(q)}
	newEntryItem := h.findCloserEntryPoint(entryPointItem, q, 0)

	numNearestToQAtBase, err := h.searchLevel(q, newEntryItem, ef, 0)

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

func (h *Hnsw) Link(friendItem *Item, node *Node, level uint) {
	dist := node.VecDistFromNode(h.Nodes[friendItem.id])

	// update both friends
	friend, ok := h.Nodes[friendItem.id]

	if !ok {
		panic("should not happen")
	}

	if friend.HasLevel(level) && node.HasLevel(level) {
		friend.InsertFriendsAtLevel(level, node.id, dist)
		node.InsertFriendsAtLevel(level, friend.id, dist)
	}
}

func (h *Hnsw) findCloserEntryPoint(ep *Item, q Vector, qLevel uint) *Item {
	for level := h.MaxLevel; level > qLevel; level-- {
		friends := h.Nodes[ep.id].GetFriendsAtLevel(level)

		for _, friend := range friends.items {
			friendDist := h.Nodes[friend.id].VecDistFromVec(q)

			if friendDist < ep.dist {
				ep = &Item{id: friend.id, dist: friend.dist}
			}
		}
	}
	return ep
}

func (h *Hnsw) Insert(q Vector) error {

	ep := h.Nodes[h.EntryNodeId]
	currentTopLevel := ep.level

	// 1. build Node for vec q
	qLevel := h.spawnLevel()
	qNode := NewNode(h.getNextNodeId(), q, qLevel)

	epItem := &Item{id: ep.id, dist: ep.VecDistFromVec(q)}

	// 2. find the correct entry point
	newEpItem := h.findCloserEntryPoint(epItem, q, qLevel)

	// 3. make the second pass, this time create connections
	for level := min(currentTopLevel, qLevel); level >= 0; level-- {
		nnToQAtLevel, err := h.searchLevel(q, newEpItem, h.EfConstruction, level)
		if err != nil {
			return fmt.Errorf("failed to make connections, %v", err)
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
			qNode.InsertFriendsAtLevel(level, peeled.id, peeled.dist)
		}
	}

	// 4. add qNode into the `Nodes` table
	h.Nodes[qNode.id] = qNode

	// 5. Link connections
	for level := min(currentTopLevel, qLevel); level >= 0; level-- {
		friendsAtLevel := qNode.GetFriendsAtLevel(level)

		for !friendsAtLevel.IsEmpty() {
			qfriend, err := friendsAtLevel.Peel()
			if err != nil {
				return err
			}
			h.Link(qfriend, qNode, level)

			qFriendNode := h.Nodes[qfriend.id]
			qFriendNodeFriendsAtLevel := qFriendNode.GetFriendsAtLevel(level)
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
					return fmt.Errorf("failed to take friend id %v's %v at level %v", qfriend.id, amt, level)
				}

				// shrink connections for a friend at level
				h.Nodes[qfriend.id].friends[level] = pq
			}
		}
	}

	// 6. update attr
	if h.MaxLevel < qLevel {
		h.MaxLevel = qLevel
		h.EntryNodeId = qNode.id
	}

	return nil
}
