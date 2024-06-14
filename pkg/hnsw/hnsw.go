package hnsw

import (
	"fmt"
	"math"
	"math/rand"
)

type Id = uint

var ErrNodeNotFound = fmt.Errorf("node not found")

type Hnsw struct {
	vectorDimensionality int

	entryPointId Id

	points  map[Id]*Point
	friends map[Id]*Friends

	levelMultiplier float64

	// efConstruction is the size of the dynamic candidate list
	efConstruction int

	// default number of connections
	M, Mmax0 int
}

func NewHnsw(d int, efConstruction int, M int, entryPoint Point) *Hnsw {
	if d <= 0 || len(entryPoint) != d {
		panic("invalid vector dimensionality")
	}

	defaultEntryPointId := Id(0)

	friends := make(map[Id]*Friends)
	friends[defaultEntryPointId] = NewFriends(0)

	points := make(map[Id]*Point)
	points[defaultEntryPointId] = &entryPoint

	return &Hnsw{
		entryPointId:         defaultEntryPointId,
		points:               points,
		vectorDimensionality: d,
		friends:              friends,
		efConstruction:       efConstruction,
		M:                    M,
		Mmax0:                2 * M,
		levelMultiplier:      1 / math.Log(float64(M)),
	}
}

func (h *Hnsw) SpawnLevel() int {
	return int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier))
}

func (h *Hnsw) GenerateId() Id {
	return Id(len(h.points))
}

func (h *Hnsw) searchLevel(q *Point, entryItem *Item, numNearestToQToReturn, level int) (*DistHeap, error) {
	visited := make([]bool, len(h.friends)+1)

	candidatesForQ := NewDistHeap()
	foundNNToQ := NewDistHeap() // this is a max

	// note entryItem.dist should be the distance to Q
	candidatesForQ.Insert(entryItem.id, entryItem.dist)
	foundNNToQ.Insert(entryItem.id, entryItem.dist)

	for !candidatesForQ.IsEmpty() {
		closestCandidate, err := candidatesForQ.PopMinItem()
		if err != nil {
			return nil, fmt.Errorf("error during searching level %d: %w", level, err)
		}

		furthestFoundNN, err := foundNNToQ.PeekMaxItem()
		if err != nil {
			return nil, fmt.Errorf("error during searching level %d: %w", level, err)
		}

		// if distance(c, q) > distance(f, q)
		if closestCandidate.dist > furthestFoundNN.dist {
			// all items in furthest found nn are evaluated
			break
		}

		closestCandidateFriends, err := h.friends[closestCandidate.id].GetFriendsAtLevel(level)
		if err != nil {
			return nil, fmt.Errorf("error during searching level %d: %w", level, err)
		}

		for _, ccFriendItem := range closestCandidateFriends.items {
			ccFriendId := ccFriendItem.id
			if !visited[ccFriendId] {
				visited[ccFriendId] = true

				furthestFoundNN, err = foundNNToQ.PeekMaxItem()
				if err != nil {
					return nil, fmt.Errorf("error during searching level %d: %w", level, err)
				}

				ccFriendPoint, ok := h.points[ccFriendId]
				if !ok {
					return nil, ErrNodeNotFound
				}

				// if distance(ccFriend, q) < distance(f, q)
				ccFriendDistToQ := EuclidDistance(*ccFriendPoint, *q)
				if ccFriendDistToQ < furthestFoundNN.dist || foundNNToQ.Len() < numNearestToQToReturn {
					candidatesForQ.Insert(ccFriendId, ccFriendDistToQ)
					foundNNToQ.Insert(ccFriendId, ccFriendDistToQ)

					if foundNNToQ.Len() > numNearestToQToReturn {
						if _, err = foundNNToQ.PopMaxItem(); err != nil {
							return nil, fmt.Errorf("error during searching level %d: %w", level, err)
						}
					}
				}
			}
		}

	}

	return foundNNToQ, nil
}

func (h *Hnsw) findCloserEntryPoint(q *Point, qFriends *Friends) *Item {
	initialEntryPoint, ok := h.friends[h.entryPointId]
	if !ok {
		panic(ErrNodeNotFound)
	}

	entryPointDistToQ := EuclidDistance(*h.points[h.entryPointId], *q)

	epItem := &Item{id: h.entryPointId, dist: entryPointDistToQ}
	for level := initialEntryPoint.TopLevel(); level > qFriends.TopLevel()+1; level-- {
		closestNeighborsToQ, err := h.searchLevel(q, epItem, 1, level)
		if err != nil {
			panic(err)
		}

		if closestNeighborsToQ.IsEmpty() {
			// return the existing epItem. it's the closest to q.
			return epItem
		}

		newEpItem, err := closestNeighborsToQ.PopMinItem()
		if err != nil {
			panic(err)
		}

		epItem = newEpItem
	}

	return epItem
}

func (h *Hnsw) selectNeighbors(nearestNeighbors *DistHeap) ([]*Item, error) {
	if nearestNeighbors.Len() <= h.M {
		return nearestNeighbors.items, nil
	}

	nearestItems := make([]*Item, h.M)

	for i := 0; i < h.M; i++ {
		nearestItem, err := nearestNeighbors.PopMinItem()

		if err != nil {
			return nil, err
		}

		nearestItems[i] = nearestItem
	}

	return nearestItems, nil
}

func (h *Hnsw) InsertVector(q Point) error {
	if !h.isValidPoint(q) {
		return fmt.Errorf("invalid vector dimensionality")
	}

	topLevel := h.friends[h.entryPointId].TopLevel()

	qId := h.GenerateId()
	qTopLevel := h.SpawnLevel()

	if qTopLevel < 0 {
		panic("invalid top level cannot have a negative top level")
	}

	qFriends := NewFriends(qTopLevel)
	h.friends[qId] = qFriends
	h.points[qId] = &q

	entryItem := h.findCloserEntryPoint(&q, qFriends)

	for level := min(topLevel, qTopLevel); level >= 0; level-- {
		nnToQAtLevel, err := h.searchLevel(&q, entryItem, h.efConstruction, level)

		if err != nil {
			return fmt.Errorf("failed to search for nearest neighbors to Q at level %v: %w", level, err)
		}

		neighbors, err := h.selectNeighbors(nnToQAtLevel)
		if err != nil {
			return fmt.Errorf("failed to select for nearest neighbors to Q at level %v: %w", level, err)
		}

		// add bidirectional connections from neighbors to q at layer c
		for _, neighbor := range neighbors {
			neighborPoint := h.points[neighbor.id]
			distNeighToQ := EuclidDistance(*neighborPoint, q)
			h.friends[neighbor.id].InsertFriendsAtLevel(level, qId, distNeighToQ)
			h.friends[qId].InsertFriendsAtLevel(level, neighbor.id, distNeighToQ)
		}

		for _, neighbor := range neighbors {
			neighborFriendsAtLevel, err := h.friends[neighbor.id].GetFriendsAtLevel(level)
			if err != nil {
				return fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", level, err)
			}

			for neighborFriendsAtLevel.Len() > h.M {
				_, err := neighborFriendsAtLevel.PopMaxItem()
				if err != nil {
					return fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", level, err)
				}

				eConnections = FromItems(items, MinComparator{})
			}

			h.friends[neighbor.id].friends[level] = neighborFriendsAtLevel
		}

		newEntryItem, err := nnToQAtLevel.PopMinItem()
		if err != nil {
			return fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", level, err)
		}

		entryItem = newEntryItem
	}

	if qTopLevel > topLevel {
		h.entryPointId = qId
	}

	return nil
}

func (h *Hnsw) isValidPoint(point Point) bool {
	return len(point) == h.vectorDimensionality
}

func (h *Hnsw) KnnSearch(q Point, numNeighborsToReturn int) (*DistHeap, error) {
	entryPoint, ok := h.points[h.entryPointId]

	if !ok {
		return nil, fmt.Errorf("no point found for entry point %v", h.entryPointId)
	}

	entryPointFriends, ok := h.friends[h.entryPointId]
	if !ok {
		return nil, fmt.Errorf("no friends found for entry point %v", h.entryPointId)
	}

	entryPointTopLevel := entryPointFriends.TopLevel()

	entryItem := &Item{
		id:   h.entryPointId,
		dist: EuclidDistance(q, *entryPoint),
	}

	for level := entryPointTopLevel; level > 0; level-- {
		nearestNeighborQueueAtLevel, err := h.searchLevel(&q, entryItem, 1, level)

		if err != nil {
			return nil, fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", level, err)
		}

		entryItem, err = nearestNeighborQueueAtLevel.PeekMinItem()
		if err != nil {
			return nil, fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", level, err)
		}
	}

	// level 0
	nearestNeighborQueueAtLevel0, err := h.searchLevel(&q, entryItem, h.efConstruction, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to find nearest neighbor to Q at level %v: %d", h.entryPointId, 0)
	}

	for nearestNeighborQueueAtLevel0.Len() > numNeighborsToReturn {
		_, err := nearestNeighborQueueAtLevel0.PopMaxItem()
		if err != nil {
			return nil, fmt.Errorf("failed to find nearest neighbor to Q at level %v: %w", h.entryPointId, err)
		}
	}

	return nearestNeighborQueueAtLevel0, nil
}
