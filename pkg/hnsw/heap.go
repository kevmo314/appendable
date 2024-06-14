package hnsw

import (
	"fmt"
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

func NewDistHeap() *DistHeap {
	d := &DistHeap{
		items:   make([]*Item, 0),
		visited: make(map[Id]int),
	}
	return d
}

func (d *DistHeap) Init() {
	n := d.Len()
	for i := n/2 - 1; i >= 0; i-- {
		d.down(i, n)
	}
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
		index = d.up(d.Len() - 1)
		d.visited[id] = index
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
	idxI, idxJ := d.visited[d.items[i].id], d.visited[d.items[j].id]
	d.visited[d.items[i].id], d.visited[d.items[j].id] = idxJ, idxI
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
