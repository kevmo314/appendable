package hnsw

import (
	"reflect"
	"testing"
)

func TestHeap(t *testing.T) {

	t.Run("basic min max properties", func(t *testing.T) {
		h := NewDistHeap()

		for i := 10; i > 0; i-- {
			h.Insert(Id(i), float32(10-i))
		}

		if h.Len() != 10 {
			t.Fatalf("heap length should be 10, got %v", h.Len())
		}

		expectedId := Id(10)
		for !h.IsEmpty() {
			peekMinItem, err := h.PeekMinItem()
			if err != nil {
				t.Fatalf("failed to peek min item: %v", err)
			}

			minItem, err := h.PopMinItem()
			if err != nil {
				t.Fatalf("failed to pop min item, err: %v", err)
			}

			if peekMinItem.id != minItem.id {
				t.Fatalf("mismatched item id, expected %v, got %v", expectedId, peekMinItem.id)
			}

			if minItem.id != expectedId {
				t.Fatalf("mismatched ids, expected %v, got: %v", expectedId, minItem.id)
			}

			expectedId -= 1
		}
	})

	t.Run("basic min max properties 2", func(t *testing.T) {
		h := NewDistHeap()

		for i := 0; i <= 10; i++ {
			h.Insert(Id(i), float32(10-i))
		}

		maxExpectedId := Id(0)
		minExpectedId := Id(10)

		for !h.IsEmpty() {
			peekMaxItem, err := h.PeekMaxItem()

			if err != nil {
				t.Fatalf("failed to peek max item, err: %v", err)
			}

			maxItem, err := h.PopMaxItem()

			if err != nil {
				t.Fatalf("failed to pop max item, err: %v", err)
			}

			if peekMaxItem.id != maxItem.id {
				t.Fatalf("mismatched max ids, expected %v, got: %v", maxItem.id, peekMaxItem.id)
			}

			if maxItem.id != maxExpectedId {
				t.Fatalf("expected id to be %v, got %v", maxExpectedId, maxItem.id)
			}

			if h.IsEmpty() {
				continue
			}

			peekMinItem, err := h.PeekMinItem()
			if err != nil {
				t.Fatalf("failed to peek min item, err: %v", err)
			}

			minItem, err := h.PopMinItem()

			if err != nil {
				t.Fatalf("failed to pop min item, err: %v", err)
			}

			if peekMinItem.id != minItem.id {
				t.Fatalf("mismatched min ids, expected %v, got: %v", maxItem.id, peekMaxItem.id)
			}

			if minItem.id != minExpectedId {
				t.Fatalf("expected id to be %v, got %v", minExpectedId, minItem.id)
			}

			minExpectedId -= 1
			maxExpectedId += 1
		}
	})

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

	t.Run("copy", func(t *testing.T) {
		m := NewDistHeap()

		for i := 0; i <= 10; i++ {
			m.Insert(Id(i), float32(10-i))
		}

		n := m.Clone()

		reflect.DeepEqual(m.items, n.items)
		reflect.DeepEqual(m.visited, n.visited)

		expectedId := Id(10)

		for !n.IsEmpty() {
			item, err := n.PopMinItem()
			if err != nil {
				return
			}

			if item.id != expectedId {
				t.Fatalf("expected id to be %v, got %v", expectedId, item.id)
			}

			expectedId -= 1
		}
	})
}

func furthestBuildings(heights []int, bricks, ladders int) (int, error) {

	ladderJumps := NewDistHeap()

	for idx := 0; idx < len(heights)-1; idx++ {
		height := heights[idx]
		nextHeight := heights[idx+1]

		if height >= nextHeight {
			continue
		}

		jump := nextHeight - height

		ladderJumps.Insert(Id(idx), float32(jump))

		if ladderJumps.Len() > ladders {
			minLadderJump, err := ladderJumps.PopMinItem()
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
