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
			offset, err := m.GetNextSlot()
			if err != nil {
				t.Fatal(err)
			}

			s[i] = offset
		}
	})
}
