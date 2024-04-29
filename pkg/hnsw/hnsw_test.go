package hnsw

import "testing"

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		h := NewHNSW(20, 32, 32)
	})
}
