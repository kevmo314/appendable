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
	efc := uint(4)

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

		g.friends[Id(0)].InsertFriendsAtLevel(0, 1, EuclidDistance(mPoint, entryPoint))
		g.friends[Id(1)] = NewFriends(0)
		g.friends[Id(1)].InsertFriendsAtLevel(0, 0, EuclidDistance(mPoint, entryPoint))

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

		entryPoint, ok := g.points[Id(0)]
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

		entryPoint, ok := g.points[Id(0)]
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
