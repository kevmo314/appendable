package btree

import "testing"

func TestMultiBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := newSeekableBuffer()
		tree := NewMultiBPTree(b)
		exists, err := tree.Exists()
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("expected not found")
		}
	})

	t.Run("reset tree", func(t *testing.T) {
		b := newSeekableBuffer()
		tree := NewMultiBPTree(b)
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
		if mp.Offset != 0 {
			t.Fatalf("expected offset 0, got %d", mp.Offset)
		}
		if mp.Length != 36 {
			t.Fatalf("expected length 36, got %d", mp.Length)
		}
	})
}
