package btree

import (
	"reflect"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
)

func TestMultiBPTree(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
		exists, err := tree.Exists()
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("expected not found")
		}
	})

	t.Run("reset tree", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
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
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
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
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
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
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
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
		p, err := NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}
		tree := NewMultiBPTree(p, uint64(p.PageSize()))
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
}
