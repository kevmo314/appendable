package hnsw

import (
	"testing"
)

func TestPQ(t *testing.T) {

	t.Run("bricks and ladders || min heap", func(t *testing.T) {
		type Case struct {
			heights  []int
			bricks   int
			ladders  int
			expected int
		}

		cases := [3]Case{
			{
				heights:  []int{4, 2, 7, 6, 9, 14, 12},
				bricks:   5,
				ladders:  1,
				expected: 4,
			},
			{
				heights:  []int{4, 12, 2, 7, 3, 18, 20, 3, 19},
				bricks:   10,
				ladders:  2,
				expected: 7,
			},
			{
				heights:  []int{14, 3, 19, 3},
				bricks:   17,
				ladders:  0,
				expected: 3,
			},
		}

		for _, c := range cases {
			res, err := furthestBuildings(c.heights, c.bricks, c.ladders)
			if err != nil {
				t.Fatal(err)
			}

			if res != c.expected {
				t.Errorf("got %d, want %d", res, c.expected)
			}
		}
	})

	t.Run("interchange", func(t *testing.T) {
		bq := NewBaseQueue(MinComparator{})
		for i := 0; i < 100; i++ {
			bq.Insert(Id(i), float32(i))
		}

		incBq := FromBaseQueue(bq, MaxComparator{})

		i := Id(99)
		for !incBq.IsEmpty() {
			item, err := incBq.PopItem()
			if err != nil {
				t.Fatal(err)
			}

			if item.id != i {
				t.Fatalf("got %d, want %d", item.id, i)
			}

			i -= 1
		}
	})

	t.Run("from items", func(t *testing.T) {
		items := []*Item{
			{id: 0, dist: 30},
			{id: 1, dist: 29},
			{id: 2, dist: 28},
			{id: 3, dist: 27},
			{id: 4, dist: 26},
			{id: 5, dist: 25},
		}

		bq := FromItems(items, MinComparator{})

		if bq.IsEmpty() {
			t.Fatal("empty queue")
		}

		if bq.Len() != len(items) {
			t.Fatalf("got %d, want %d", bq.Len(), len(items))
		}

		expectedId := Id(5)
		for !bq.IsEmpty() {
			bqItem, err := bq.PopItem()
			if err != nil {
				t.Fatal(err)
			}

			if bqItem.id != expectedId {
				t.Fatalf("got %d, want %d", bqItem.id, expectedId)
			}

			expectedId -= 1
		}

	})
}

func furthestBuildings(heights []int, bricks, ladders int) (int, error) {

	ladderJumps := NewBaseQueue(MinComparator{})

	for idx := 0; idx < len(heights)-1; idx++ {
		height := heights[idx]
		nextHeight := heights[idx+1]

		if height >= nextHeight {
			continue
		}

		jump := nextHeight - height

		ladderJumps.Insert(Id(idx), float32(jump))

		if ladderJumps.Len() > ladders {
			minLadderJump, err := ladderJumps.PopItem()
			if err != nil {
				return -1, err
			}

			if bricks-int(minLadderJump.dist) < 0 {
				return idx, nil
			}

			bricks -= int(minLadderJump.dist)
		}
	}

	return len(heights) - 1, nil
}
