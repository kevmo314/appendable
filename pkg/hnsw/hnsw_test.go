package hnsw

import (
	"errors"
	"reflect"
	"testing"
)

var clusterA = []Point{
	{0.2, 0.5},
	{0.2, 0.7},
	{0.3, 0.8},
	{0.5, 0.5},
	{0.4, 0.1},
	{0.3, 0.7},
	{0.27, 0.23},
	{0.12, 0.1},
	{0.23, 0.25},
	{0.3, 0.3},
	{0.01, 0.3},
}

var clusterB = []Point{
	{4.2, 3.5},
	{4.2, 4.7},
	{4.3, 3.8},
	{4.5, 4.5},
	{4.4, 3.1},
	{4.3, 4.7},
	{4.27, 3.23},
	{4.1, 4.1},
	{4, 2},
	{4.23, 4.25},
	{4.3, 3.3},
	{4.01, 4.3},
}

func SetupClusterHnsw(cluster []Point) (*Hnsw, error) {
	efc := 4

	entryPoint := Point{0, 0}
	g := NewHnsw(2, efc, 4, entryPoint)

	for idx, point := range cluster {
		pointId := Id(idx + 1)
		g.points[pointId] = &point
		g.friends[pointId] = NewFriends(0)

		distEntryToClusterPoint := EuclidDistance(entryPoint, point)
		g.friends[g.entryPointId].InsertFriendsAtLevel(0, pointId, distEntryToClusterPoint)
		g.friends[pointId].InsertFriendsAtLevel(0, g.entryPointId, distEntryToClusterPoint)
	}

	for idx, pointA := range cluster {
		for jdx, pointB := range cluster {
			if idx == jdx {
				continue
			}

			pointAId := Id(idx + 1)
			pointBId := Id(jdx + 1)

			distAToB := EuclidDistance(pointA, pointB)
			g.friends[pointAId].InsertFriendsAtLevel(0, pointBId, distAToB)
			g.friends[pointBId].InsertFriendsAtLevel(0, pointAId, distAToB)
		}
	}

	for kdx := range cluster {
		pointId := Id(kdx + 1)
		friends, err := g.friends[pointId].GetFriendsAtLevel(0)
		if err != nil {
			return nil, err
		}

		for friends.Len() > int(efc) {
			friends.Pop()
		}

		if friends.Len() != int(efc) {
			return nil, errors.New("not all friends length follow the efc parameter")
		}
	}

	return g, nil
}

func TestHnsw_SearchLevel(t *testing.T) {
	t.Run("search level 0", func(t *testing.T) {
		entryPoint := Point{0, 0}
		g := NewHnsw(2, 4, 4, entryPoint)
		mPoint := Point{2, 2}
		g.points[Id(1)] = &mPoint

		g.friends[g.entryPointId].InsertFriendsAtLevel(0, 1, EuclidDistance(mPoint, entryPoint))
		g.friends[Id(1)] = NewFriends(0)
		g.friends[Id(1)].InsertFriendsAtLevel(0, g.entryPointId, EuclidDistance(mPoint, entryPoint))

		qPoint := Point{4, 4}
		closestNeighbor, err := g.searchLevel(&qPoint, &Item{id: 0, dist: EuclidDistance(entryPoint, qPoint)}, 1, 0)
		if err != nil {
			t.Fatal(err)
		}

		if closestNeighbor.IsEmpty() {
			t.Fatalf("expected # of neighbors to return to be 1, got %v", closestNeighbor)
		}

		closestItem, err := closestNeighbor.PopItem()

		if err != nil {
			t.Fatal(err)
		}

		if Id(1) != closestItem.id {
			t.Fatalf("expected item id to be %v, got %v", 1, closestItem.id)
		}
	})

	t.Run("cluster a searchLayer for existing point", func(t *testing.T) {
		g, err := SetupClusterHnsw(clusterA)

		if err != nil {
			t.Fatal(err)
		}

		entryPoint, ok := g.points[g.entryPointId]
		if !ok {
			t.Fatal(ErrNodeNotFound)
		}

		qPoint := clusterA[3]
		expectedId := Id(4)

		expectedPoint := g.points[expectedId]

		if !reflect.DeepEqual(qPoint, *expectedPoint) {
			t.Fatalf("expected point to be %v, got %v", expectedPoint, qPoint)
		}

		closestNeighbor, err := g.searchLevel(&qPoint, &Item{
			id:   0,
			dist: EuclidDistance(*entryPoint, qPoint),
		}, 1, 0)

		if err != nil {
			t.Fatal(err)
		}

		if closestNeighbor.IsEmpty() {
			t.Fatalf("expected # of neighbors to return to be 1, got %v", closestNeighbor)
		}

		closestItem, err := closestNeighbor.PopItem()

		if err != nil {
			t.Fatal(err)
		}

		if closestItem.id != expectedId {
			t.Fatalf("expected the closest item to be the 3rd point in Cluster a, got %v", closestItem.id)
		}
	})

	t.Run("cluster a searchLayer for new point", func(t *testing.T) {
		g, err := SetupClusterHnsw(clusterA)

		if err != nil {
			t.Fatal(err)
		}

		entryPoint, ok := g.points[g.entryPointId]
		if !ok {
			t.Fatal(ErrNodeNotFound)
		}

		qPoint := Point{0.3, 0.81}
		expectedId := Id(3) // point3 is (0.3, 0.8)

		closestNeighbor, err := g.searchLevel(&qPoint, &Item{
			id:   0,
			dist: EuclidDistance(*entryPoint, qPoint),
		}, 1, 0)

		if err != nil {
			t.Fatal(err)
		}

		if closestNeighbor.IsEmpty() {
			t.Fatalf("expected # of neighbors to return to be 1, got %v", closestNeighbor)
		}

		closestItem, err := closestNeighbor.PopItem()

		if err != nil {
			t.Fatal(err)
		}

		if closestItem.id != expectedId {
			t.Fatalf("expected the closest item to be the 3rd point in Cluster a, got %v", closestItem.id)
		}
	})

	t.Run("cluster a, b, selectLayer and return the closest point", func(t *testing.T) {
		clusterC := append(append([]Point{}, clusterA...), clusterB...)
		g, err := SetupClusterHnsw(clusterC)
		if err != nil {
			t.Fatal(err)
		}

		qPoint := Point{2, 2}

		closestNeighbor, err := g.searchLevel(&qPoint, &Item{id: 0, dist: EuclidDistance(Point{0, 0}, qPoint)}, 1, 0)

		if err != nil {
			t.Fatal(err)
		}

		if closestNeighbor.IsEmpty() {
			t.Fatalf("expected # of neighbors to return to be 1, got %v", closestNeighbor.Len())
		}

		closestItem, err := closestNeighbor.PopItem()
		if err != nil {
			t.Fatal(err)
		}

		if closestItem.id != Id(20) {
			t.Fatalf("expected the closest point which is {4, 2} and id %v, got %v", Id(20), closestItem.id)
		}
	})

	t.Run("cluster a, b, selectLayer and return the closest points from both clusters", func(t *testing.T) {
		clusterC := append(append([]Point{}, clusterA...), clusterB...)
		g, err := SetupClusterHnsw(clusterC)
		if err != nil {
			t.Fatal(err)
		}

		qPoint := Point{2, 2}

		closestNeighbor, err := g.searchLevel(&qPoint, &Item{id: 0, dist: EuclidDistance(Point{0, 0}, qPoint)}, 4, 0)
		if err != nil {
			t.Fatal(err)
		}

		if closestNeighbor.IsEmpty() {
			t.Fatalf("expected # of neighbors to return to be 1, got %v", closestNeighbor.Len())
		}

		if closestNeighbor.Len() != 4 {
			t.Fatalf("expected # of neighbors to return to be %v, got %v", 4, closestNeighbor.Len())
		}

		var closestIds []Id

		for !closestNeighbor.IsEmpty() {
			closestItem, err := closestNeighbor.PopItem()
			if err != nil {
				t.Fatal(err)
			}

			closestIds = append(closestIds, closestItem.id)
		}

		if !reflect.DeepEqual(closestIds, []Id{20, 3, 4, 6}) {
			t.Fatalf("expected the following closest ids: %v, got: %v", []Id{20, 3, 4, 6}, closestIds)
		}
	})
}

func TestHnsw_FindCloserEntryPoint(t *testing.T) {
	t.Run("finds closer point", func(t *testing.T) {
		h := NewHnsw(2, 4, 4, Point{0, 0})

		/*
			Before anything, we need to pad the entry node's friends queue to include more than level 0.
			This is because we only consider the following topLevels

			for level := initialEntryPoint.TopLevel(); level > qFriends.TopLevel()+1; level-- {
		*/

		h.friends[h.entryPointId] = NewFriends(4)

		closerPointId := Id(1)
		closerPoint := Point{2, 2}

		h.points[closerPointId] = &closerPoint
		h.friends[closerPointId] = NewFriends(4)

		distToEntry := EuclidDistance(Point{0, 0}, closerPoint)
		h.friends[closerPointId].InsertFriendsAtLevel(4, h.entryPointId, distToEntry)
		h.friends[h.entryPointId].InsertFriendsAtLevel(4, closerPointId, distToEntry)

		closestItem := h.findCloserEntryPoint(&Point{4, 4}, NewFriends(0))

		if closestItem.id != closerPointId {
			t.Fatalf("expected closest item to be %v, got %v", closerPointId, closestItem.id)
		}

		if !NearlyEqual(closestItem.dist, EuclidDistance(Point{2, 2}, Point{4, 4})) {
			t.Fatalf("expected the closest item dist to be %v, got %v", closestItem.dist, EuclidDistance(Point{2, 2}, Point{4, 4}))
		}

	})

	t.Run("single level means entry point is the closest", func(t *testing.T) {
		h := NewHnsw(2, 4, 4, Point{0, 0})

		h.friends[h.entryPointId] = NewFriends(4)

		closerPointId := Id(1)
		closerPoint := Point{2, 2}

		h.points[closerPointId] = &closerPoint
		h.friends[closerPointId] = NewFriends(4)

		distToEntry := EuclidDistance(Point{0, 0}, closerPoint)

		// since we're inserting friends at the same level as q, it will return entry point
		h.friends[closerPointId].InsertFriendsAtLevel(0, Id(0), distToEntry)
		h.friends[Id(0)].InsertFriendsAtLevel(0, closerPointId, distToEntry)

		closestItem := h.findCloserEntryPoint(&Point{4, 4}, NewFriends(0))

		if closestItem.id != Id(0) {
			t.Fatalf("expected closest item to be %v, got %v", closerPointId, closestItem.id)
		}
	})
}

func TestHnsw_SelectNeighbors(t *testing.T) {

	t.Run("selects neighbors given overflow", func(t *testing.T) {
		nearestNeighbors := NewBaseQueue(MinComparator{})

		M := 4

		h := NewHnsw(2, 4, M, Point{0, 0})

		// since M is 4
		for i := 5; i >= 0; i-- {
			nearestNeighbors.Insert(Id(i), float32(i))
		}

		neighbors, err := h.selectNeighbors(nearestNeighbors)

		if err != nil {
			t.Fatal(err)
		}

		if len(neighbors) != M {
			t.Fatalf("select neighbors should have at most M friends")
		}

		// for the sake of testing, let's rebuild the pq and assert ids are correct
		reneighbors := NewBaseQueue(MinComparator{})

		for _, item := range neighbors {
			reneighbors.Insert(item.id, item.dist)
		}

		expectedId := Id(0)
		for !reneighbors.IsEmpty() {
			nn, err := reneighbors.PopItem()

			if err != nil {
				t.Fatal(err)
			}

			if nn.id != expectedId {
				t.Fatalf("expected item to be %v, got %v", expectedId, nn.id)
			}

			expectedId += 1
		}
	})

	t.Run("selects neighbors given lower bound", func(t *testing.T) {
		M := 10
		h := NewHnsw(2, 10, M, Point{0, 0})

		nnQueue := NewBaseQueue(MinComparator{})

		for i := 0; i < 3; i++ {
			nnQueue.Insert(Id(i), float32(i))
		}

		neighbors, err := h.selectNeighbors(nnQueue)

		if err != nil {
			t.Fatal(err)
		}

		if len(neighbors) != 3 {
			t.Fatalf("select neighbors should have at least 3 neighbors, got: %v", len(neighbors))
		}

		reneighbors := NewBaseQueue(MinComparator{})

		for _, item := range neighbors {
			reneighbors.Insert(item.id, item.dist)
		}

		expectedId := Id(0)
		for !reneighbors.IsEmpty() {
			nn, err := reneighbors.PopItem()

			if err != nil {
				t.Fatal(err)
			}

			if nn.id != expectedId {
				t.Fatalf("expected item to be %v, got %v", expectedId, nn.id)
			}

			expectedId += 1
		}
	})
}

func TestHnsw_InsertVector(t *testing.T) {
	t.Run("basic insert", func(t *testing.T) {
		h := NewHnsw(2, 3, 4, Point{0, 0})
		q := Point{3, 3}

		if len(q) != 2 {
			t.Fatal("insert vector should have 2 elements")
		}

		err := h.InsertVector(q)

		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bulk insert", func(t *testing.T) {
		items := 1

		h := NewHnsw(3, 4, 4, Point{0, 0, 0})

		for i := 100; i >= 1; i-- {
			j := float32(i)
			q := Point{j, j, j}

			if len(h.friends) != items {
				t.Fatalf("expected # of friends to be %v, got %v", items-1, len(h.friends))
			}

			if len(h.friends) != len(h.points) {
				t.Fatalf("expected friends and points map to have same length throughout insertion")
			}

			err := h.InsertVector(q)
			if err != nil {
				return
			}

			if len(h.friends) != len(h.points) {
				t.Fatalf("expected friends and points map to have same length throughout insertion")
			}

			if len(h.friends) != items+1 {
				t.Fatalf("expected # of friends to be %v, got %v", items+1, len(h.friends))
			}

			items += 1
		}

	})
}

func TestHnsw_KnnSearch(t *testing.T) {
	t.Run("basic search knn", func(t *testing.T) {
		h := NewHnsw(2, 4, 4, Point{0, 0})

		// id: 1
		if err := h.InsertVector(Point{3, 3}); err != nil {
			t.Fatalf("failed to insert point: %v, err: %v", Point{3, 3}, err)
		}

		// id: 2
		if err := h.InsertVector(Point{4, 4}); err != nil {
			t.Fatalf("failed to insert point %v, err: %v", Point{4, 4}, err)
		}

		// id: 3
		if err := h.InsertVector(Point{5, 5}); err != nil {
			t.Fatalf("failed to insert point %v, err: %v", Point{5, 5}, err)
		}

		nearestNeighbors, err := h.KnnSearch(Point{5, 5}, 3)
		if err != nil {
			t.Fatal(err)
		}

		if nearestNeighbors.Len() != 3 {
			t.Fatalf("expected to have 3 neighbors, got %v", nearestNeighbors)
		}

		expectedId := Id(3)

		for !nearestNeighbors.IsEmpty() {
			nearestNeighbor, err := nearestNeighbors.PopItem()
			if err != nil {
				t.Fatalf("failed to pop item: %v, err: %v", nearestNeighbors, err)
			}

			if nearestNeighbor.id != expectedId {
				t.Fatalf("expected item to be %v, got %v", expectedId, nearestNeighbor.id)
			}

			expectedId -= 1
		}
	})
}
