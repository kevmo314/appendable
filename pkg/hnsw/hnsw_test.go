package hnsw

import "testing"

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		h := NewHNSW(20, 32, 32)

		if h.MaxLayer != -1 {
			t.Fatalf("expected max layer to default to -1, got %v", h.MaxLayer)
		}
	})
}
