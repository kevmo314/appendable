package hnsw

import "testing"

/*
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
	{4.12, 3.1},
	{4.23, 4.25},
	{4.3, 3.3},
	{4.01, 4.3},
}
*/

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
}
