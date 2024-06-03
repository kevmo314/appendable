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
		hex := []float32{9, 2.0, 30}

		hexFriends := NewFriends(6)

		if hexFriends.TopLevel() != 6 {
			t.Fatalf("since 0-indexed, the max level is 5, got: %v", hexFriends.TopLevel())
		}

		if hexFriends.NumLevels() != 7 {
			t.Fatalf("since 0-indexed, the number of levels is 6, got: %v", hexFriends.NumLevels())
		}

		octId := Id(2)
		oct := []float32{0, 2, 3}
		octFriends := NewFriends(8)

		if octFriends.TopLevel() != 8 {
			t.Fatalf("since 0-indexed, the max level is 7, got: %v", octFriends.TopLevel())
		}

		if octFriends.NumLevels() != 9 {
			t.Fatalf("since 0-indexed, the number of levels is 8, got: %v", octFriends.NumLevels())
		}

		for i := 0; i <= 6; i++ {
			if !hexFriends.HasLevel(i) {
				t.Fatalf("since 0-indexed, the level #%v is missing", i)
			}
		}

		for i := 7; i <= 8; i++ {
			if hexFriends.HasLevel(i) {
				t.Fatalf("since 0-indexed, expected the level #%v to be missing", i)
			}
		}

		hexOctDist := EuclidDistance(oct, hex)

		hexFriends.InsertFriendsAtLevel(5, octId, hexOctDist)
		octFriends.InsertFriendsAtLevel(5, hexId, hexOctDist)

		for i := 0; i <= 5; i++ {
			hexFriends, err := hexFriends.GetFriendsAtLevel(i)
			if err != nil {
				t.Fatal(err)
			}

			octFriends, err := octFriends.GetFriendsAtLevel(i)
			if err != nil {
				t.Fatal(err)
			}

			if hexFriends.Len() != 1 || octFriends.Len() != 1 {
				t.Fatalf("expected hex and oct friends list at level %v to be 1, got: %v || %v", i, hexFriends.Len(), octFriends.Len())
			}

			top := hexFriends.Top()
			if top.id != octId {
				t.Fatalf("expected %v, got %v", octId, top.id)
			}

			top = octFriends.Top()
			if top.id != hexId {
				t.Fatalf("expected %v, got %v", hexId, top.id)
			}
		}
	})

}

func TestVector_EuclidDistance(t *testing.T) {

	type vectorPair struct {
		v0, v1   Point
		expected float32
	}

	basic := []vectorPair{
		{
			v0:       Point{5, 3, 0},
			v1:       Point{2, -2, float32(math.Sqrt(2))},
			expected: 6,
		},
		{
			v0:       Point{1, 0, -5},
			v1:       Point{-3, 2, -1},
			expected: 6,
		},
		{
			v0:       Point{1, 3},
			v1:       Point{5, 2},
			expected: float32(math.Sqrt(17)),
		},
		{
			v0:       Point{0, 1, 4},
			v1:       Point{2, 9, 1},
			expected: float32(math.Sqrt(77)),
		},
		{
			v0:       Point{0},
			v1:       Point{0},
			expected: 0,
		},
		{
			v0:       Point{10, 20, 30, 40},
			v1:       Point{10, 20, 30, 40},
			expected: 0,
		},
	}

	t.Run("correctly computes the distance of two vectors", func(t *testing.T) {
		for i, pair := range basic {
			dist := EuclidDistance(pair.v1, pair.v0)

			if !NearlyEqual(dist, pair.expected) {
				t.Fatalf("iter i: %v, expected %v and %v to be equal", i, dist, pair.expected)
			}
		}
	})
}
