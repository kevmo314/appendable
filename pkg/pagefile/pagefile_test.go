package pagefile

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
		offset, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset)
		}
	})

	t.Run("page size allocates pages", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset1)
		}
		// check the seek location
		n, err := buf.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if n != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, n)
		}
		offset2, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset2 != pageSizeBytes*2 {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes*2, offset2)
		}
		m, err := buf.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if m != pageSizeBytes*2 {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes*2, m)
		}
	})

	t.Run("page size allocates page with data", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		data := []byte("hello")
		offset1, err := pf.NewPage(data, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset1 != pageSizeBytes {
			t.Fatalf("expected offset %d, got %d", pageSizeBytes, offset1)
		}
		if _, err := pf.Seek(offset1, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf2 := make([]byte, len(data))
		if _, err := pf.Read(buf2); err != nil {
			t.Fatal(err)
		}
		if string(buf2) != string(data) {
			t.Fatalf("expected %s, got %s", string(data), string(buf2))
		}
	})

	t.Run("new page seeks to page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset1, err := pf.NewPage(nil, nil)
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
		offset1, err := pf.NewPage(nil, nil)
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
		offset2, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset2 != pageSizeBytes*2 {
			t.Fatalf("expected offset %d, got %d", 2*pageSizeBytes, offset2)
		}

		if err := pf.FreePage(offset1); err != nil {
			t.Fatal(err)
		}
		offset3, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if offset3 != offset1 {
			t.Fatalf("expected offset %d, got %d", offset2, offset3)
		}
	})

	t.Run("free page behaves like a circular buffer", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offsets := make([]int64, 0, 10)
		for i := 0; i < 10; i++ {
			offset, err := pf.NewPage(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if i > 0 && offset != offsets[i-1]+pageSizeBytes {
				t.Fatalf("expected offset %d, got %d", offsets[i-1]+pageSizeBytes, offset)
			}
			offsets = append(offsets, offset)
		}
		for i := 0; i < 10; i++ {
			if err := pf.FreePage(offsets[i]); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			offset, err := pf.NewPage(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if offset != offsets[i] {
				t.Fatalf("expected offset %d, got %d", offsets[i], offset)
			}
		}
	})

	t.Run("cannot double free a page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		offset, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := pf.FreePage(offset); err != nil {
			t.Fatal(err)
		}
		if err := pf.FreePage(offset); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("track number of pages", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}
		if pf.PageCount() != 1 {
			t.Fatalf("expected 1, got %d", pf.PageCount())
		}
		offset, err := pf.NewPage(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pf.PageCount() != 2 {
			t.Fatalf("expected 2, got %d", pf.PageCount())
		}
		if err := pf.FreePage(offset); err != nil {
			t.Fatal(err)
		}
		if pf.PageCount() != 2 {
			t.Fatalf("expected 2, got %d", pf.PageCount())
		}
		if _, err := pf.NewPage(nil, nil); err != nil {
			t.Fatal(err)
		}
		if pf.PageCount() != 2 {
			t.Fatalf("expected 2, got %d", pf.PageCount())
		}
		if _, err := pf.NewPage(nil, nil); err != nil {
			t.Fatal(err)
		}
		if pf.PageCount() != 3 {
			t.Fatalf("expected 3, got %d", pf.PageCount())
		}
	})
}
