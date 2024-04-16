package metapage

import (
	"reflect"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
)

func TestMultiBTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		pages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(pages) > 0 {
			t.Fatal("expected not found")
		}
	})

	t.Run("reset tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tree.AddNext(); err != nil {
			t.Fatal(err)
		}
		pages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(pages) != 1 {
			t.Fatalf("expected to find %d pages, got %d", 1, len(pages))
		}
	})

	t.Run("insert a second page", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}

		next1, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		next2, err := next1.AddNext()
		if err != nil {
			t.Fatal(err)
		}

		if reflect.DeepEqual(next1, next2) {
			t.Fatalf("expected different pages, got %v", next1)
		}

		pages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(pages) != 2 {
			t.Fatalf("expected to find %d pages, got %d", 2, len(pages))
		}

		// check the first page
		if !reflect.DeepEqual(pages[0], next1) {
			t.Fatalf("got %v, want %v", pages[0], next1)
		}
		if !reflect.DeepEqual(pages[1], next2) {
			t.Fatalf("got %v, want %v", pages[1], next2)
		}
	})

	t.Run("duplicate next pointer", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tree.AddNext(); err != nil {
			t.Fatal(err)
		}

		if _, err := tree.AddNext(); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("can't store metadata on a tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.SetMetadata([]byte("hello")); err != errNotAPage {
			t.Fatal("expected error")
		}
		if _, err := tree.Metadata(); err != errNotAPage {
			t.Fatal("expected error")
		}
	})

	t.Run("can't store root on a tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.SetRoot(pointer.MemoryPointer{}); err != errNotAPage {
			t.Fatal("expected error")
		}
		if _, err := tree.Root(); err != errNotAPage {
			t.Fatal("expected error")
		}
	})

	t.Run("storing metadata works", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		node, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if err := node.SetMetadata([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		metadata, err := node.Metadata()
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
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		node, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}
		if err := node.SetMetadata(make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
		if err := node.SetMetadata(make([]byte, 257)); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("creating at least 15 pages works", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}

		node := tree
		for i := 0; i < 16; i++ {
			next, err := node.AddNext()
			if err != nil {
				t.Fatal(err)
			}
			node = next
		}

		pages, err := tree.Collect()
		if err != nil {
			t.Fatal(err)
		}
		if len(pages) != 16 {
			t.Fatalf("expected to find %d pages, got %d", 16, len(pages))
		}
	})
}
