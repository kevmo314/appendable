package metapage

import (
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"testing"
)

func TestMetaPager(t *testing.T) {
	t.Run("allocate meta pages within a single page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)

		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)
		nm := m.rws.PageSize() / m.rws.SlotSize()

		if nm != 16 {
			t.Fatalf("number of meta page slots should be 16, got: %v", nm)
		}
	})

	t.Run("allocate meta pages within a single page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)
		nm := m.rws.PageSize() / m.rws.SlotSize()
		if nm != 16 {
			t.Fatalf("number of meta page slots should be 16, got: %v", nm)
		}

		s := make([]int64, nm)

		for i := 0; i < nm; i++ {
			offset, err := m.GetNextSlot()
			if err != nil {
				t.Fatal(err)
			}

			s[i] = offset
		}
	})

	t.Run("sets the first slot to filled", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)

		tree, err := NewMultiBPTree(pf, m, 0)
		if err != nil {
			t.Fatal(err)
		}

		if len(m.freeSlotIndexes) != 1 {
			t.Fatalf("expect length of free slot indexes to be 1, got %v", len(m.freeSlotIndexes))
		}

		if !m.freeSlotIndexes[0][0] {
			t.Fatal("expected 0, 0 to be filled as true")
		}

		if tree.offset != 4096 {
			t.Fatal("expected tree offset to be 4096")
		}
	})

	t.Run("fills up one page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)

		tree, err := NewMultiBPTree(pf, m, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}

		prevSlot := tree
		for i := 1; i < 16; i++ { // we start from 1 because tree was technically 0th
			newSlot, err := prevSlot.AddNext()
			if err != nil {
				t.Fatal(err)
			}

			prevSlot = newSlot

			if m.rws.PageCount() != 1 {
				t.Fatal("expect only one page to occur")
			}
		}
		if m.rws.PageCount() != 1 {
			t.Fatal("expect only one page to occur")
		}

		if len(m.freeSlotIndexes) != 1 {
			t.Fatalf("expected free slot indexes to be of length 1, got %v", len(m.freeSlotIndexes))
		}

		slots, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(slots) != 16 {
			t.Fatalf("expected # of slots to be %v, got %v", 16, len(slots))
		}

		for i, sc := range m.freeSlotIndexes[0] {
			if !sc {
				t.Fatalf("expected the slot to be filled at %v", i)
			}
		}

		// let's also assert slots offsets increment by 256

		var prevOffset int64
		prevOffset = -1

		for i, slot := range slots {
			if prevOffset == -1 {
				prevOffset = int64(slot.offset)
			} else {
				if uint64(prevOffset)+256 != slot.offset {
					t.Fatalf("expected offset to be %v, got %v at %v", prevOffset+256, slot.offset, i)
				}
			}

			prevOffset = int64(slot.offset)
		}
	})

	t.Run("fills up many pages", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)

		tree, err := NewMultiBPTree(pf, m, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}

		N := 200

		prevSlot := tree
		for i := 1; i < N; i++ { // we start from 1 because tree was technically 0th
			newSlot, err := prevSlot.AddNext()
			if err != nil {
				t.Fatal(err)
			}

			prevSlot = newSlot

			if m.rws.PageCount() != int64(i/16)+1 {
				t.Fatalf("expect %v pages, got %v at %v", int64(i/16), m.rws.PageCount(), i)
			}
		}
		if m.rws.PageCount() != int64(1+N/16) {
			t.Fatalf("expect %v pages, got: %v", int64(1+N/16), m.rws.PageCount())
		}

		if len(m.freeSlotIndexes) != 1+N/16 {
			t.Fatalf("expected free slot indexes to be of length %v, got %v", 1+N/16, len(m.freeSlotIndexes))
		}

		var prevOffset int64
		prevOffset = -1

		slots, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(slots) != N {
			t.Fatalf("expected # of slots to be %v, got %v", 16, len(slots))
		}

		for i, slot := range slots {
			if prevOffset == -1 {
				prevOffset = int64(slot.offset)
			} else {
				if uint64(prevOffset)+256 != slot.offset {
					t.Fatalf("expected offset to be %v, got %v at %v", prevOffset+256, slot.offset, i)
				}
			}

			prevOffset = int64(slot.offset)
		}
	})
}
