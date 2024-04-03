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

	t.Run("insert a second slot", func(t *testing.T) {
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

		// check the first slot
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

	t.Run("collect slots", func(t *testing.T) {
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
		slot1, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		slot2, err := slot1.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		slot3, err := slot2.AddNext()
		if err != nil {
			t.Fatal(err)
		}

		// Collect the slots
		collectedSlots, err := slot1.Collect()
		if err != nil {
			t.Fatal(err)
		}

		// Verify the collected pages
		expectedSlots := []*LinkedMetaSlot{slot1, slot2, slot3}
		if !reflect.DeepEqual(collectedSlots, expectedSlots) {
			t.Fatalf("got %v, want %v", collectedSlots, expectedSlots)
		}
	})

	t.Run("test m slots for n pages", func(t *testing.T) {
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

		sn := 30

		ps := tree
		for i := 0; i < sn; i++ {
			ns, err := ps.AddNext()
			if err != nil {
				t.Fatal(err)
			}
			ps = ns
		}

		slot1, err := tree.Next()
		if err != nil {
			t.Fatal(err)
		}

		collectedSlots, err := slot1.Collect()
		if err != nil {
			t.Fatal(err)
		}

		if len(collectedSlots) != sn {
			t.Fatalf("expected # of slots to be %v, got %v", sn, len(collectedSlots))
		}

		if 1+(sn/16) != (int)(ms.PageCount()) {
			t.Errorf("expected %v pages, got %v", (int)(ms.PageCount()), 1+(sn/16))
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
		collectedSlots, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		expectedSlots := []*LinkedMetaSlot{tree}
		if !reflect.DeepEqual(collectedSlots, expectedSlots) {
			t.Fatalf("got %v, want %v", collectedSlots, expectedSlots)
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
		collectedSlots, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if collectedSlots != nil {
			t.Fatalf("got %v, want nil", collectedSlots)
		}
	})

}
