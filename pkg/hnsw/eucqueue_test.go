package hnsw

import (
	"testing"
)

type Pair struct {
	id   NodeId
	dist float64
}

func TestEucQueue(t *testing.T) {
	t.Run("builds min queue", func(t *testing.T) {
		v0 := Vector{1.0}

		vs := [5]Vector{
			{2.3}, // id: 0, dist: 1.3, p: 4
			{1.1}, // id: 1, dist: 0.1, p: 1
			{2.0}, // id: 2, dist: 1.0, p: 3
			{3.3}, // id: 3, dist: 2.3, p: 5
			{0.8}, // id: 4, dist: 0.2, p: 2
		}

		eq := NewMinQueue()
		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)
		}

		expected := [5]Pair{
			{1, 0.1},
			{4, 0.2},
			{2, 1.0},
			{0, 1.3},
			{3, 2.3},
		}

		i := 0
		for eq.Len() > 0 {
			item := eq.Peel()
			if item.id != expected[i].id || !NearlyEqual(item.dist, expected[i].dist) {
				t.Fatalf("expected item %v, got %v at %v", expected[i].id, item.id, i)
			}

			i++
		}
	})

	t.Run("builds max queue", func(t *testing.T) {
		v0 := Vector{1.0}

		vs := [5]Vector{
			{2.3}, // id: 0, dist: 1.3, p: 4
			{1.1}, // id: 1, dist: 0.1, p: 1
			{2.0}, // id: 2, dist: 1.0, p: 3
			{3.3}, // id: 3, dist: 2.3, p: 5
			{0.8}, // id: 4, dist: 0.2, p: 2
		}

		eq := NewMaxQueue()
		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)
		}

		expected := [5]Pair{
			{3, 2.3},
			{0, 1.3},
			{2, 1.0},
			{4, 0.2},
			{1, 0.1},
		}

		i := 0
		for eq.Len() > 0 {
			item := eq.Peel()
			if item.id != expected[i].id || !NearlyEqual(item.dist, expected[i].dist) {
				t.Fatalf("expected item id: %v, got id: %v at i: %v", expected[i].id, item.id, i)
			}

			i++
		}
	})
}
