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

	// efConstruction is the size of the dynamic candIdate list
	efConstruction uint

	// default number of connections
	M, Mmax0 int
}

func NewHnsw(d int, efConstruction uint, M int, entryPoint Point) *Hnsw {
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
	return int(math.Floor(-math.Log(rand.Float64() * h.levelMultiplier)))
}

func (h *Hnsw) searchLevel(q *Point, entryItem *Item, numNearestToQToReturn, level int) (*BaseQueue, error) {
	visited := make([]bool, len(h.friends)+1)

	candidatesForQ := NewBaseQueue(MinComparator{})
	foundNNToQ := NewBaseQueue(MaxComparator{})

	// note entryItem.dist should be the distance to Q
	candidatesForQ.Insert(entryItem.id, entryItem.dist)
	foundNNToQ.Insert(entryItem.id, entryItem.dist)

	for !candidatesForQ.IsEmpty() {
		closestCandidate, err := candidatesForQ.PopItem()
		if err != nil {
			return nil, fmt.Errorf("error during searching level %d: %w", level, err)
		}

		furthestFoundNN := foundNNToQ.Top()

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

				furthestFoundNN = foundNNToQ.Top()

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
						if _, err = foundNNToQ.PopItem(); err != nil {
							return nil, fmt.Errorf("error during searching level %d: %w", level, err)
						}
					}
				}
			}
		}

	}

	return FromBaseQueue(foundNNToQ, MinComparator{}), nil
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

		newEpItem, err := closestNeighborsToQ.PopItem()
		if err != nil {
			panic(err)
		}

		epItem = newEpItem
	}

	return epItem
}

func (h *Hnsw) InsertVector(q Point) error {
	if !h.validatePoint(q) {
		return fmt.Errorf("invalid vector dimensionality")
	}

	qTopLevel := h.SpawnLevel()
	qFriends := NewFriends(qTopLevel)

	_ = h.findCloserEntryPoint(&q, qFriends)
	return nil
}

func (h *Hnsw) validatePoint(point Point) bool {
	return len(point) != h.vectorDimensionality
}
