package hnsw

import (
	"math"
	"testing"
)

func TestWithinLevels(t *testing.T) {
	t.Run("levels are in bounds", func(t *testing.T) {
		n := NewNode(3, []float64{3, 6, 9}, 3)

		n.friends[0] = NewBaseQueue(MinComparator{})
		n.friends[1] = NewBaseQueue(MinComparator{})
		n.friends[2] = NewBaseQueue(MinComparator{})

		for i := 0; i < 3; i++ {
			if !n.HasLevel(uint(i)) {
				t.Fatalf("since n's max level is %v, all levels less should be true", n.level)
			}
		}

		if n.HasLevel(3 + 1) {
			t.Fatalf("since n's max level is %v, levels greater is not in bounds", n.level)
		}
	})
}

func TestVec(t *testing.T) {

	type t_case struct {
		u, v     []float64
		expected float64
	}

	bank := [7]t_case{
		{
			u:        []float64{5, 3, 0},
			v:        []float64{2, -2, math.Sqrt(2)},
			expected: 6,
		},
		{
			u:        []float64{1, 0, -5},
			v:        []float64{-3, 2, -1},
			expected: 6,
		},
		{
			u:        []float64{1, 3},
			v:        []float64{5, 2},
			expected: math.Sqrt(17),
		},
		{
			u:        []float64{0, 1, 4},
			v:        []float64{2, 9, 1},
			expected: math.Sqrt(77),
		},
		{
			u:        []float64{0},
			v:        []float64{0},
			expected: 0,
		},
		{
			u:        []float64{10, 20, 30, 40},
			v:        []float64{10, 20, 30, 40},
			expected: math.Sqrt(0),
		},
	}

	t.Run("correctly computes the dist from node", func(t *testing.T) {
		for i, bank := range bank {

			if !NearlyEqual(bank.expected, EuclidDist(bank.u, bank.v)) {
				t.Fatalf("err at %v, expected %v, got %v", i, bank.expected, EuclidDist(bank.u, bank.v))
			}
		}
	})

	t.Run("symmetric", func(t *testing.T) {
		for i, bank := range bank {

			if !NearlyEqual(EuclidDist(bank.v, bank.u), EuclidDist(bank.u, bank.v)) {
				t.Fatalf("err at %v, expected %v, got %v", i, bank.expected, EuclidDist(bank.u, bank.v))
			}
		}
	})
}

func TestNodeFriends(t *testing.T) {
	t.Run("initialized with correct # of levels", func(t *testing.T) {
		h := NewHNSW(20, 32, 32, NewNode(0, []float64{3, 4}, 8))
		qLayer := h.spawnLevel()
		qNode := NewNode(1, []float64{3, 1}, qLayer)

		if uint(len(qNode.friends)) != qLayer+1 {
			t.Fatalf("expected the friends list to initialize to %v levels, got %v", qLayer+1, len(qNode.friends))
		}
	})

	t.Run("correctly determines if has layer", func(t *testing.T) {
		qNode := NewNode(10, []float64{3, 1, 0.3, 9.2}, 100)

		if !qNode.HasLevel(100) {
			t.Fatalf("expected qNode to have level %v", 100)
		}

		if qNode.HasLevel(101) {
			t.Fatalf("expected qNode to not have level %v", 101)
		}

		if !qNode.HasLevel(0) {
			t.Fatalf("expected qNode to have level %v", 0)
		}
	})

}
