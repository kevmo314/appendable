package hnsw

import (
	"fmt"
	"maps"
	"math/bits"
)

type Item struct {
	id   Id
	dist float32
}

var EmptyHeapError = fmt.Errorf("Empty Heap")

type DistHeap struct {
	items   []*Item
	visited map[Id]int
}

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

func NewDistHeap() *DistHeap {
	d := &DistHeap{
		items:   make([]*Item, 0),
		visited: make(map[Id]int),
	}
	return d
}

func (d *DistHeap) Clone() *DistHeap {
	n := &DistHeap{
		items:   make([]*Item, len(d.items)),
		visited: make(map[Id]int, len(d.visited)),
	}

	copy(n.items, d.items)
	maps.Copy(n.visited, d.visited)

	return n
}

func (d *DistHeap) PeekMinItem() (*Item, error) {
	if d.IsEmpty() {
		return nil, EmptyHeapError
	}

	return d.items[0], nil
}
func (d *DistHeap) PeekMaxItem() (*Item, error) {
	if d.Len() == 0 {
		return nil, EmptyHeapError
	}

	// Find the maximum element without removing it
	n := d.Len()

	i := 0
	l := lchild(0)
	if l < n && !d.Less(l, i) {
		i = l
	}

	r := rchild(0)
	if r < n && !d.Less(r, i) {
		i = r
	}

	return d.items[i], nil
}
func (d *DistHeap) PopMinItem() (*Item, error) {
	if d.IsEmpty() {
		return nil, EmptyHeapError
	}

	n := d.Len() - 1
	d.Swap(0, n)
	d.down(0, n)
	return d.Pop(), nil
}
func (d *DistHeap) PopMaxItem() (*Item, error) {
	if d.IsEmpty() {
		return nil, EmptyHeapError
	}

	n := d.Len()
	i := 0
	l := lchild(0)

	if l < n && !d.Less(l, i) {
		i = l
	}

	r := rchild(0)
	if r < n && !d.Less(r, i) {
		i = r
	}

	d.Swap(i, n-1)
	d.down(i, n-1)

	return d.Pop(), nil
}
func (d *DistHeap) Insert(id Id, dist float32) {
	index, ok := d.visited[id]

	if !ok {
		d.Push(&Item{id: id, dist: dist})
		d.visited[id] = d.Len() - 1
		d.up(d.Len() - 1)
		return
	}

	d.items[index].dist = dist
	d.Fix(index)
}

func (d *DistHeap) Fix(i int) {
	if !d.down(i, d.Len()) {
		d.up(i)
	}
}

func (d DistHeap) IsEmpty() bool      { return len(d.items) == 0 }
func (d DistHeap) Len() int           { return len(d.items) }
func (d DistHeap) Less(i, j int) bool { return d.items[i].dist < d.items[j].dist }
func (d DistHeap) Swap(i, j int) {
	d.visited[d.items[i].id], d.visited[d.items[j].id] = j, i
	d.items[i], d.items[j] = d.items[j], d.items[i]
}
func (d *DistHeap) Push(x *Item) {
	(*d).items = append((*d).items, x)
}
func (d *DistHeap) Pop() *Item {
	old := (*d).items
	n := len(old)
	x := old[n-1]
	(*d).items = old[0 : n-1]
	delete(d.visited, x.id)
	return x
}
