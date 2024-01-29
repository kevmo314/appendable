package btree

import (
	"io"
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
)

func TestPageFile(t *testing.T) {
	t.Run("allocates first page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset)
		}
	})

	t.Run("page size reuses page without allocation", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset1)
		}
		// since no data has been written, this page should be reused.
		offset2, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset2 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes*2, offset2)
		}
	})

	t.Run("page size allocates second page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset1)
		}
		// need to write at least one byte to trigger a new page.
		if _, err := pf.Write(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		offset2, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset2 != pageSizeBytes*2 {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes*2, offset2)
		}
	})

	t.Run("new page seeks to page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		offset2, err := pf.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != offset2 {
			t.Fatalf("expected offset %d, got %d", offset1, offset2)
		}
	})

	t.Run("free page reuses page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset1)
		}
		// need to write at least one byte to trigger a new page.
		if _, err := pf.Write(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		offset2, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset2 != pageSizeBytes*2 {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset2)
		}

		if err := pf.FreePage(offset1); err != nil {
			t.Fatal(err)
		}
		offset3, err := pf.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		if offset3 != offset1 {
			t.Fatalf("expected offset %d, got %d", offset2, offset3)
		}
	})
}
