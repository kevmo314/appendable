// Package minmaxheap provides min-max heap operations for any type that
// implements heap.Interface. A min-max heap can be used to implement a
// double-ended priority queue.
//
// Min-max heap implementation from the 1986 paper "Min-Max Heaps and
// Generalized Priority Queues" by Atkinson et. al.
// https://doi.org/10.1145/6617.6621.

package hnsw

import (
	"math/bits"
)

func level(i int) int {
	// floor(log2(i + 1))
	return bits.Len(uint(i)+1) - 1
}

func isMinLevel(i int) bool {
	return level(i)%2 == 0
}

func lchild(i int) int {
	return i*2 + 1
}

func rchild(i int) int {
	return i*2 + 2
}

func parent(i int) int {
	return (i - 1) / 2
}

func hasParent(i int) bool {
	return i > 0
}

func hasGrandparent(i int) bool {
	return i > 2
}

func grandparent(i int) int {
	return parent(parent(i))
}

func (d *DistHeap) down(i, n int) bool {
	min := isMinLevel(i)
	i0 := i
	for {
		m := i

		l := lchild(i)
		if l >= n || l < 0 /* overflow */ {
			break
		}
		if d.Less(l, m) == min {
			m = l
		}

		r := rchild(i)
		if r < n && d.Less(r, m) == min {
			m = r
		}

		// grandchildren are contiguous i*4+3+{0,1,2,3}
		for g := lchild(l); g < n && g <= rchild(r); g++ {
			if d.Less(g, m) == min {
				m = g
			}
		}

		if m == i {
			break
		}

		d.Swap(i, m)

		if m == l || m == r {
			break
		}

		// m is grandchild
		p := parent(m)
		if d.Less(p, m) == min {
			d.Swap(m, p)
		}
		i = m
	}
	return i > i0
}

func (d *DistHeap) up(i int) {
	min := isMinLevel(i)

	if hasParent(i) {
		p := parent(i)
		if d.Less(p, i) == min {
			d.Swap(i, p)
			min = !min
			i = p
		}
	}

	for hasGrandparent(i) {
		g := grandparent(i)
		if d.Less(i, g) != min {
			return
		}

		d.Swap(i, g)
		i = g
	}
}
