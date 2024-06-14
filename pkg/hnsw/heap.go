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
	visited map[Id]bool
}

func NewDistHeap() *DistHeap {
	d := &DistHeap{
		items:   make([]*Item, 0),
		visited: make(map[Id]bool),
	}
	return d
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
	if d.visited[id] {
		for idx, item := range d.items {
			if item.id == id {
				item.dist = dist
				d.Fix(idx)
				return
			}
		}
	} else {
		d.Push(&Item{id: id, dist: dist})
		d.up(d.Len() - 1)
		d.visited[id] = true
	}
}
func (d *DistHeap) Fix(i int) {
	if !d.down(i, d.Len()) {
		d.up(i)
	}
}

func (d DistHeap) IsEmpty() bool      { return len(d.items) == 0 }
func (d DistHeap) Len() int           { return len(d.items) }
func (d DistHeap) Less(i, j int) bool { return d.items[i].dist < d.items[j].dist }
func (d DistHeap) Swap(i, j int)      { d.items[i], d.items[j] = d.items[j], d.items[i] }
func (d *DistHeap) Push(x *Item) {
	(*d).items = append((*d).items, x)
}
func (d *DistHeap) Pop() *Item {
	old := (*d).items
	n := len(old)
	x := old[n-1]
	(*d).items = old[0 : n-1]
	return x
}
