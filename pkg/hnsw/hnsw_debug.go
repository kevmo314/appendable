//go:build debug
// +build debug

package hnsw

import "fmt"

// should never exceed MMAX, at level 0 shouldn't exceed MMAX0
func (h *Hnsw) assertNeighbors() (bool, error) {

	for nodeId, node := range h.Nodes {

		if len(node.friends) == 0 {
			return false, fmt.Errorf("node %v shouldn't have 0 friends", nodeId)
		}

		for level := len(node.friends) - 1; level >= 0; level-- {
			if level == 0 && node.friends[level].Len() > h.MMax0 {
				return false, fmt.Errorf("node %v has too many friends", nodeId)
			} else if node.friends[level].Len() > h.MMax {
				return false, fmt.Errorf("node %v has too many friends", nodeId)
			}
		}

	}

	return true, nil
}
