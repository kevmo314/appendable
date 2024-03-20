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
			offset, err := m.NextSlot(nil)
			if err != nil {
				t.Fatal(err)
			}

			s[i] = offset
			if offset/int64(m.rws.PageSize()) != 0 {
				t.Fatalf("expected all meta pages to be in page 0, got page: %v", offset/int64(m.rws.PageSize()))
			}

		}
	})

	t.Run("reuse freed meta page", func(t *testing.T) {
		buf := buftest.NewSeekableBuffer()
		pf, err := pagefile.NewPageFile(buf)
		if err != nil {
			t.Fatal(err)
		}

		m := New(pf)

		n := 26
		offset, err := m.NextSlot(nil)
		if err != nil {
			t.Fatal(err)
		}

		if offset != 0 {
			t.Fatalf("expected offset: 0, got: %v", offset)
		}

		var lastOffset int64 = -1

		for i := 0; i < n; i++ {
			offset, err = m.NextSlot(nil)
			if err != nil {
				t.Fatal(err)
			}

			if i == 0 {
				lastOffset = offset
			} else {
				expectedOffset := lastOffset + int64(m.rws.SlotSize())

				if offset != expectedOffset {
					t.Fatalf("expected recycled offset: %v, got %v at step %v", expectedOffset, offset, i)
				}
				lastOffset = offset
			}

		}
		if pf.PageCount() != 2 {
			t.Fatalf("expected page count to be 2, got: %v", pf.PageCount())
		}
	})
}
