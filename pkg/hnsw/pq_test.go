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
			minLadderJump, err := ladderJumps.Peel()
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

/*



















 */
