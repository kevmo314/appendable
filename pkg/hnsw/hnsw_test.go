package hnsw

import "testing"

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		n := NewNode(0, Vector([]float64{0.1, 0.2}))
		h := NewHNSW(20, 32, 32, n)

		if h.MaxLayer != -1 {
			t.Fatalf("expected max layer to default to -1, got %v", h.MaxLayer)
		}
	})
}
