package pointer

import (
	"slices"
	"testing"
)

func TestReferencedValue(t *testing.T) {
	t.Run("compare referenced value", func(t *testing.T) {
		keys := []ReferencedId{
			{
				Value: 1,
				DataPointer: MemoryPointer{
					Offset: 100,
					Length: 0,
				},
			},
			{
				Value: 2,
				DataPointer: MemoryPointer{
					Offset: 200,
					Length: 0,
				},
			},
			{
				Value: 3,
				DataPointer: MemoryPointer{
					Offset: 300,
					Length: 0,
				},
			},
		}

		index, found := slices.BinarySearchFunc(keys, ReferencedId{
			DataPointer: MemoryPointer{},
			Value:       1,
		}, CompareReferencedIds)

		if !found {
			t.Fatal("expected to find key 1")
		}

		index++
		if index != 1 {
			t.Fatalf("expected index to be 1, got: %v", index)
		}
	})

}
