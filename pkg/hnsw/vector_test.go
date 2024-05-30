package hnsw

import (
	"math"
	"testing"
)

func TestVector_LevelManagement(t *testing.T) {

	/*
		hex has 6 layers from [0..5]
		oct has 8 layers from [0..8]
	*/
	t.Run("check levels for oct and hex vectors", func(t *testing.T) {
		hexId := Id(1)
		hex := NewVector(hexId, []float32{9, 2.0, 30}, 6)

		if hex.MaxLevel() != 6 {
			t.Fatalf("since 0-indexed, the max level is 5, got: %v", hex.MaxLevel())
		}

		if hex.Levels() != 7 {
			t.Fatalf("since 0-indexed, the number of levels is 6, got: %v", hex.Levels())
		}

		octId := Id(2)
		oct := NewVector(octId, []float32{0, 2, 3}, 8)

		if oct.MaxLevel() != 8 {
			t.Fatalf("since 0-indexed, the max level is 7, got: %v", hex.MaxLevel())
		}

		if oct.Levels() != 9 {
			t.Fatalf("since 0-indexed, the number of levels is 8, got: %v", hex.Levels())
		}

		for i := 0; i <= 6; i++ {
			if !hex.HasLevel(i) {
				t.Fatalf("since 0-indexed, the level #%v is missing", i)
			}
		}

		for i := 7; i <= 8; i++ {
			if hex.HasLevel(i) {
				t.Fatalf("since 0-indexed, expected the level #%v to be missing", i)
			}
		}

		hex.InsertFriendsAtLevel(5, oct)

		for level, friends := range hex.friends {
			if level <= 5 {
				if friends.Len() != 1 {
					t.Fatalf("expected 1 item, got %v", friends.Len())
				}
			} else {
				if friends.Len() != 0 {
					t.Fatalf("expected 0 items, got %v", friends.Len())
				}
			}
		}
	})

}

func TestVector_EuclidDistance(t *testing.T) {

	type vectorPair struct {
		v0, v1   *Vector
		expected float32
	}

	basic := []vectorPair{
		{
			v0:       NewVector(0, []float32{5, 3, 0}, 4),
			v1:       NewVector(1, []float32{2, -2, float32(math.Sqrt(2))}, 4),
			expected: 6,
		},
		{
			v0:       NewVector(1, []float32{1, 0, -5}, 3),
			v1:       NewVector(2, []float32{-3, 2, -1}, 3),
			expected: 6,
		},
		{
			v0:       NewVector(1, []float32{1, 3}, 20),
			v1:       NewVector(1, []float32{5, 2}, 120),
			expected: float32(math.Sqrt(17)),
		},
		{
			v0:       NewVector(1, []float32{0, 1, 4}, 10),
			v1:       NewVector(2, []float32{2, 9, 1}, 100),
			expected: float32(math.Sqrt(77)),
		},
		{
			v0:       NewVector(1, []float32{0}, 9),
			v1:       NewVector(2, []float32{0}, 8),
			expected: 0,
		},
		{
			v0:       NewVector(1, []float32{10, 20, 30, 40}, 4),
			v1:       NewVector(2, []float32{10, 20, 30, 40}, 3),
			expected: 0,
		},
	}

	t.Run("correctly computes the distance of two vectors", func(t *testing.T) {
		for i, pair := range basic {
			dist := pair.v0.EuclidDistance(pair.v1)

			if !NearlyEqual(dist, pair.expected) {
				t.Fatalf("iter i: %v, expected %v and %v to be equal", i, dist, pair.expected)
			}
		}
	})
}
