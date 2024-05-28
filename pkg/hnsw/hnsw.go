package hnsw

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
)

type Hnsw struct {
	vectorDim uint

	Nodes map[NodeId]*Node

	EntryNodeId NodeId
	NextNodeId  NodeId

	MaxLevel int

	// default number of connections
	M int

	//  Maximum number of connections per element per level
	MMax, MMax0 int

	// Size of dynamic candidate list during construction
	EfConstruction int

	// Normalization factor for level generation
	levelMultiplier float64
}

func NewHNSW(d uint, m int, efc int, entryVector Vector) *Hnsw {
	h := &Hnsw{vectorDim: d}
	h.checkVectorDim(entryVector)

	nt := make(map[NodeId]*Node)
	enId := NodeId(0) // special. Reserved for the entryPointNode

	entryPoint := NewNode(enId, entryVector, 0)
	nt[enId] = entryPoint

	h.M = m
	h.Nodes = nt
	h.EntryNodeId = enId
	h.NextNodeId = enId + 1
	h.levelMultiplier = 1 / math.Log(float64(m))
	h.EfConstruction = efc
	h.MMax = m
	h.MMax0 = m * 2

	return h
}

func (h *Hnsw) checkVectorDim(v Vector) {
	if h.vectorDim != uint(len(v)) {
		panic(fmt.Sprintf("vector (%v) is invalid, expected dim length %v, got %v", v, h.vectorDim, len(v)))
	}
}

func (h *Hnsw) getNextNodeId() NodeId {
	return atomic.AddUint32(&h.NextNodeId, 1) - 1
}

func (h *Hnsw) spawnLevel() int {
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

func (h *Hnsw) searchLevel(q Vector, entryNodeItem *Item, ef int, levelId int) (*BaseQueue, error) {
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
		closestCandidate, err := candidates.Peel()
		if err != nil {
			return nil, err
		}

		furthestNN, err := nearestNeighborsToQForEf.Peek()
		if err != nil {
			return nil, err
		}

		closestCandidateToQDist := closestCandidate.dist
		furthestNNToQDist := furthestNN.dist

		if closestCandidateToQDist > furthestNNToQDist {
			break
		}

		if h.Nodes[closestCandidate.id].HasLevel(levelId) {
			friends := h.Nodes[closestCandidate.id].GetFriendsAtLevel(levelId)

			for _, friend := range friends.items {
				friendId := friend.id

				if !visited[friendId] {
					visited[friendId] = true
					furthestNNItem, err := nearestNeighborsToQForEf.Peek()

					if err != nil {
						return nil, err
					}

					friendToQDist := EuclidDist(h.Nodes[friendId].v, q)
					fmt.Printf("friend to q dist: %v", friendToQDist)

					if nearestNeighborsToQForEf.Len() < ef {
						nearestNeighborsToQForEf.Insert(friendId, friendToQDist)
						candidates.Insert(friendId, friendToQDist)
					} else if friendToQDist < furthestNNItem.dist {
						nearestNeighborsToQForEf.Pop()
						nearestNeighborsToQForEf.Insert(friendId, friendToQDist)
						candidates.Insert(friendId, friendToQDist)
					}
				}
			}
		}
	}

	return nearestNeighborsToQForEf, nil
}

func (h *Hnsw) selectNeighbors(candidates *BaseQueue, numNeighborsToReturn int) (*BaseQueue, error) {
	if candidates.Len() <= numNeighborsToReturn {
		return candidates, nil
	}

	pq, err := candidates.Take(numNeighborsToReturn, MinComparator{})
	if err != nil {
		return nil, fmt.Errorf("an error occured during take: %v", err)
	}

	return pq, nil
}

func (h *Hnsw) KnnSearch(q Vector, kNeighborsToReturn, ef int) (*BaseQueue, error) {
	currentNearestElements := NewBaseQueue(MinComparator{})
	entryPointNode := h.Nodes[h.EntryNodeId]
	entryPointItem := &Item{id: h.EntryNodeId, dist: entryPointNode.VecDistFromVec(q)}
	newEntryItem := h.findCloserEntryPoint(entryPointItem, q, 0)

	numNearestToQAtBase, err := h.searchLevel(q, newEntryItem, ef, 0)

	if err != nil {
		return nil, err
	}

	if numNearestToQAtBase.IsEmpty() {
		panic("nearest to q at base is empty")
	}

	for !numNearestToQAtBase.IsEmpty() {
		peeled, err := numNearestToQAtBase.Peel()
		if err != nil {
			return nil, err
		}
		currentNearestElements.Insert(peeled.id, peeled.dist)
	}

	if currentNearestElements.Len() < kNeighborsToReturn {
		return nil, fmt.Errorf("the currentNearestElement length %v", currentNearestElements.Len())
	}

	pq, err := currentNearestElements.Take(kNeighborsToReturn, MinComparator{})
	if err != nil {
		return nil, fmt.Errorf("failed to knnsearch, err: %v", err)
	}

	return pq, nil
}

func (h *Hnsw) Link(friendItem *Item, node *Node, level int) {
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

func (h *Hnsw) findCloserEntryPoint(ep *Item, q Vector, qLevel int) *Item {
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
	h.checkVectorDim(q)

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
