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

		if !eq.IsEmpty() || eq.Len() != 0 || len(eq.visitedIds) != 0 {
			t.Fatalf("created new eq, expected empty, got %v len", eq.Len())
		}

		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)

			if i+1 != eq.Len() {
				t.Fatalf("inserting element %v means eq should have length of %v, got: %v", i, i+1, eq.Len())
			}

			if _, ok := eq.visitedIds[NodeId(i)]; !ok {
				t.Fatalf("expected node id %v to be in visited set", i)
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
			if item.id != expected[i].id {
				t.Fatalf("expected item id %v, got %v at %v", expected[i].id, item.id, i)
			}

			if !NearlyEqual(float64(item.dist), float64(expected[i].dist)) {
				t.Fatalf("not equal, got %v, and %v", item.dist, expected[i].dist)
			}

			if _, ok := eq.visitedIds[item.id]; ok {
				t.Fatalf("expected item id %v to be popped!", item.id)
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

		if !eq.IsEmpty() || eq.Len() != 0 || len(eq.visitedIds) != 0 {
			t.Fatalf("created new eq, expected empty, got %v len", eq.Len())
		}

		for i, v := range vs {
			dist := EuclidDist(v0, v)
			eq.Insert(NodeId(i), dist)

			if i+1 != eq.Len() {
				t.Fatalf("inserting element %v means eq should have length of %v, got: %v", i, i+1, eq.Len())
			}

			if _, ok := eq.visitedIds[NodeId(i)]; !ok {
				t.Fatalf("expected node id %v to be in visited set", i)
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
			if item.id != expected[i].id || !NearlyEqual(float64(item.dist), float64(expected[i].dist)) {
				t.Fatalf("expected item id: %v, got id: %v at i: %v", expected[i].id, item.id, i)
			}

			if _, ok := eq.visitedIds[item.id]; ok {
				t.Fatalf("expected item id %v to be popped!", item.id)
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

		if len(pq.visitedIds) != 3 {
			t.Fatalf("expected # of visited ids to be %v, got %v", 3, len(pq.visitedIds))
		}

	})

	t.Run("updates already existing id with new priority", func(t *testing.T) {
		mq := FromBaseQueue([]*Item{
			{id: 1, dist: 2.2},
			{id: 2, dist: 3.0},
		}, MinComparator{})

		mq.Insert(1, 2.1)

		if mq.Len() != 2 {
			t.Fatalf("update shouldn't incur another element. expected length: %v, got: %v", 2, mq.Len())
		}

		if item, err := mq.Peek(); err != nil || item.id != 1 {
			t.Fatalf("expected first id to be 1, got: %v", item.id)
		}

		if mq.Len() != 2 {
			t.Fatalf("update shouldn't incur another element. expected length: %v, got: %v", 2, mq.Len())
		}

		if item, err := mq.Peek(); err != nil || item.dist != 2.1 {
			t.Fatalf("expected distance to be updated to %v, got %v", 2.1, item.dist)
		}

		if mq.Len() != 2 {
			t.Fatalf("update shouldn't incur another element. expected length: %v, got: %v", 2, mq.Len())
		}

		_, err := mq.Peel()

		if err != nil {
			t.Fatalf("%v", err)
		}

		if mq.Len() != 1 {
			t.Fatalf("expected length %v, got %v", 1, mq.Len())
		}

		if item, err := mq.Peek(); err != nil || item.id != 2 {
			t.Fatalf("expected second id to be 2, got %v", item.id)
		}

		if item, err := mq.Peek(); err != nil || item.dist != 3.0 {
			t.Fatalf("expected distance %v, got %v", 3.0, item.dist)
		}
	})

	t.Run("given a max heap, peeks properly", func(t *testing.T) {
		mq := FromBaseQueue([]*Item{
			{id: 1, dist: 30},
			{id: 2, dist: 40},
		}, MaxComparator{})

		if mq.Len() != 2 {
			t.Fatalf("expected length %v, got %v", 2, mq.Len())
		}

		if item, err := mq.Peek(); err != nil || item.id != 2 {
			t.Fatalf("expected the max item to be id %v, got %v", 2, item.id)
		}

		_, err := mq.Peel()

		if err != nil {
			t.Fatalf("error occured when peeling: %v", err)
		}

		if mq.Len() != 1 {
			t.Fatalf("expected length %v, got %v", 1, mq.Len())
		}

		if item, err := mq.Peek(); err != nil || item.id != 1 {
			t.Fatalf("expected the max item to be id: %v, got %v", 1, item.id)
		}

	})

	t.Run("inserting with same id yields update, not insertion", func(t *testing.T) {
		mq := FromBaseQueue([]*Item{
			{id: 1, dist: 40},
		}, MinComparator{})

		for i := 2; i <= 100; i++ {
			mq.Insert(1, float32(i))

			if mq.Len() != 1 {
				t.Fatalf("expected len to be %v, got %v", 1, mq.Len())
			}

			item, err := mq.Peek()
			if err != nil {
				t.Fatalf("error when peeking %v", err)
			}

			if !NearlyEqual(float64(item.dist), float64(i)) {
				t.Fatalf("expected distance to be the newly updated %v, got %v", i, item.dist)
			}
		}

		if mq.Len() != 1 {
			t.Fatalf("expected len to be %v, got %v", 1, mq.Len())
		}

		item, err := mq.Peek()

		if err != nil {
			t.Fatalf("error when peeking %v", err)
		}

		if !NearlyEqual(float64(item.dist), float64(100)) {
			t.Fatalf("expected distance to be the newly updated %v, got %v", 100, item.dist)
		}
	})
}
