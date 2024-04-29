package hnsw

import "testing"

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		h := New(128, 32)
		if h.maxLevel != -1 {
			t.Fatalf("expected max level to be 1, got %v", h.maxLevel)
		}

		if len(h.levels) != 0 {
			t.Fatalf("expected levels to be %v, got %v", 0, len(h.levels))
		}

		h.setDefaultProbas()
	})
}
