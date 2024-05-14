package hnsw

import (
	"testing"
)

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

		eq := NewBaseQueue(MinComparator{})

		if !eq.IsEmpty() || eq.Len() != 0 {
			t.Fatalf("created new eq, expected empty, got %v len", eq.Len())
		}

		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)

			if i+1 != eq.Len() {
				t.Fatalf("inserting element %v means eq should have length of %v, got: %v", i, i+1, eq.Len())
			}

		}

		expected := [5]Item{
			{id: 1, dist: 0.1},
			{id: 4, dist: 0.2},
			{id: 2, dist: 1.0},
			{id: 0, dist: 1.3},
			{id: 3, dist: 2.3},
		}

		i := 0
		for eq.Len() > 0 {
			item, err := eq.Peel()
			if err != nil {
				t.Fatal(err)
			}
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

		eq := NewBaseQueue(MaxComparator{})

		if !eq.IsEmpty() || eq.Len() != 0 {
			t.Fatalf("created new eq, expected empty, got %v len", eq.Len())
		}

		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)

			if i+1 != eq.Len() {
				t.Fatalf("inserting element %v means eq should have length of %v, got: %v", i, i+1, eq.Len())
			}
		}

		expected := [5]Item{
			{id: 3, dist: 2.3},
			{id: 0, dist: 1.3},
			{id: 2, dist: 1.0},
			{id: 4, dist: 0.2},
			{id: 1, dist: 0.1},
		}

		i := 0
		for eq.Len() > 0 {
			item, err := eq.Peel()
			if err != nil {
				t.Fatal(err)
			}
			if item.id != expected[i].id || !NearlyEqual(item.dist, expected[i].dist) {
				t.Fatalf("expected item id: %v, got id: %v at i: %v", expected[i].id, item.id, i)
			}

			i++
		}
	})

	t.Run("takes correctly", func(t *testing.T) {
		mq := FromBaseQueue([]*Item{
			{id: 1, dist: 33},
			{id: 2, dist: 32},
			{id: 3, dist: 69},
			{id: 4, dist: 3},
			{id: 6, dist: 0.01},
		}, MinComparator{})

		pq, err := mq.Take(3, MinComparator{})
		if err != nil {
			t.Fatalf("failed to take 3")
		}

		if pq.Len() != 3 {
			t.Fatalf("expected len: %v, got %v", 3, pq.Len())
		}

	})
}
