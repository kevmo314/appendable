package vectorpage

import (
	"github.com/kevmo314/appendable/pkg/hnsw"
	"testing"
)

func TestNewVectorPageManager(t *testing.T) {

	t.Run("", func(t *testing.T) {
		p0 := hnsw.Point{3, 3}

		h := hnsw.NewHnsw(2, 10, 8, p0)

		for i := 0; i < 100; i++ {
			if err := h.InsertVector(hnsw.Point{float32(i), float32(i)}); err != nil {
				t.Fatal(err)
			}
		}

	})
}
