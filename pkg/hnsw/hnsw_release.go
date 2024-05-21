//go:build !debug
// +build !debug

package hnsw

func (h *Hnsw) assertNeighbors() (bool, error) {
	return true, nil
}
