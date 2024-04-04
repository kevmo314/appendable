package metapage

import (
	"reflect"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

func TestMultiBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		exists, err := tree.Exists()
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("expected not found, got page %v", tree)
		}
	})

	t.Run("reset tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		exists, err := tree.Exists()
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("expected found")
		}
		mp := tree.MemoryPointer()
		if mp.Length != 24 {
			t.Fatalf("expected length 24, got %d", mp.Length)
		}
	})

	t.Run("insert a second page", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		next1, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if next1.MemoryPointer().Length != 24 {
			t.Fatalf("expected length 24, got %d", next1)
		}
		next2, err := next1.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if next2.MemoryPointer().Length != 24 {
			t.Fatalf("expected length 24, got %d", next2)
		}

		if next1.MemoryPointer().Offset == next2.MemoryPointer().Offset {
			t.Fatalf("expected different offsets, got %d", next1.MemoryPointer().Offset)
		}

		// check the first page
		m1, err := tree.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m1.MemoryPointer(), next1.MemoryPointer()) {
			t.Fatalf("got %v want %v", m1.MemoryPointer(), next1.MemoryPointer())
		}
	})

	t.Run("duplicate next pointer", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		next1, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if next1.MemoryPointer().Length != 24 {
			t.Fatalf("expected length 24, got %d", next1)
		}
		_, err = tree.AddNext()
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("starts with empty metadata", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		metadata, err := tree.Metadata()

		if err != nil {
			t.Fatal(err)
		}
		if len(metadata) != 0 {
			t.Fatalf("expected empty metadata, got %v", metadata)
		}
	})

	t.Run("storing metadata works", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		if err := tree.SetMetadata([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		metadata, err := tree.Metadata()

		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(metadata, []byte("hello")) {
			t.Fatalf("got %v want %v", metadata, []byte("hello"))
		}
	})

	t.Run("setting metadata too large fails", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		if err := tree.SetMetadata(make([]byte, 4096)); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("collect pages", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}

		// Create a linked list of LinkedMetaPages
		page1, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		page2, err := page1.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		page3, err := page2.AddNext()
		if err != nil {
			t.Fatal(err)
		}

		// Collect the pages
		collectedPages, err := page1.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// Verify the collected pages
		expectedPages := []*LinkedMetaPage{page1, page2, page3}
		if !reflect.DeepEqual(collectedPages, expectedPages) {
			t.Fatalf("got %v, want %v", collectedPages, expectedPages)
		}
	})

	t.Run("singular list", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}
		collectedPages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		expectedPages := []*LinkedMetaPage{tree}
		if !reflect.DeepEqual(collectedPages, expectedPages) {
			t.Fatalf("got %v, want %v", collectedPages, expectedPages)
		}
	})

	t.Run("empty list", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree, err := NewMultiBPTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		collectedPages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if collectedPages != nil {
			t.Fatalf("got %v, want nil", collectedPages)
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
