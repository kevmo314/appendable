package btree

import (
	"io"
	"testing"
)

func TestPageFile(t *testing.T) {
	t.Run("no page size behaves like regular ReadWriteSeeker", func(t *testing.T) {
		buf := newSeekableBuffer()
		pf := &PageFile{
			ReadWriteSeeker: buf,
		}
		if _, err := pf.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if _, err := pf.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, err := pf.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 5)
		if _, err := pf.Read(b); err != nil {
			t.Fatal(err)
		}
		if string(b) != "hello" {
			t.Fatalf("expected %q, got %q", "hello", string(b))
		}
	})

	t.Run("page size allocates pages on seek end", func(t *testing.T) {
		buf := newSeekableBuffer()
		pf := &PageFile{
			ReadWriteSeeker: buf,
			PageSize:        16,
		}
		if _, err := pf.Seek(0, io.SeekEnd); err != nil {
			t.Fatal(err)
		}
		if _, err := pf.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, err := pf.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 5)
		if _, err := pf.Read(b); err != nil {
			t.Fatal(err)
		}
		if string(b) != "hello" {
			t.Fatalf("expected %q, got %q", "hello", string(b))
		}
		n, err := pf.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if n != 16 {
			t.Fatalf("expected %d, got %d", 16, n)
		}
	})
}
