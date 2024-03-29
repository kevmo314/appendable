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

		ms := New(p)

		tree, err := NewMultiBPTree(p, ms, 0)
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

		ms := New(p)

		tree, err := NewMultiBPTree(p, ms, 0)
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
			t.Fatalf("expected length 24, got %v", next1)
		}
		next2, err := next1.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if next2.MemoryPointer().Length != 24 {
			t.Fatalf("expected length 24, got %v", next2)
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

		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
			t.Fatalf("expected length 24, got %v", next1)
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

		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.Reset(); err != nil {
			t.Fatal(err)
		}

		// Create a linked list of LinkedMetaSlots
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
		expectedPages := []*LinkedMetaSlot{page1, page2, page3}
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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
		expectedPages := []*LinkedMetaSlot{tree}
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
		ms := New(p)
		tree, err := NewMultiBPTree(p, ms, 0)
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

	/*
		t.Run("packs slot into one page", func(t *testing.T) {
			b := buftest.NewSeekableBuffer()
			p, err := pagefile.NewPageFile(b)
			if err != nil {
				t.Fatal(err)
			}
			ms := New(p)
			currPage, err := NewMultiBPTree(p, ms, 0)
			if err := currPage.Reset(); err != nil {
				t.Fatal(err)
			}

			n := 26
			for i := 0; i < n; i++ {
				nextPage, err := currPage.AddNext()
				if err != nil {
					t.Fatalf("%v at %v", err, i)
				}

				if (i/16 + 1) != int(p.PageCount()) {
					t.Fatalf("expected %v, got %v", i/16+1, p.PageCount())
				}
				currPage = nextPage
			}

			if len(ms.freeSlotIndexes) != 2 {
				t.Fatalf("expected slot indexes to be 2, got %v", len(ms.freeSlotIndexes))
			}
		})
	*/
}
