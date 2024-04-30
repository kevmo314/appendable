package hnsw

import "container/heap"

type Item struct {
	id    NodeID
	dist  float64
	index int
}

// EucQueue is a heap that can return the furthest or closest neighbor based
// on the euclidian distance of that given vector from an origin vector.
// We shouldn't go below EucQueue as everything we need has been abstracted away.
type EucQueue struct {
	queue heap.Interface
}

func NewEucQueue(min bool) *EucQueue {
	var queue heap.Interface
	if min {
		minQueue := &MinQueue{}
		heap.Init(minQueue)
		queue = minQueue
	} else {
		maxQueue := &MaxQueue{}
		heap.Init(maxQueue)
		queue = maxQueue
	}
	return &EucQueue{
		queue: queue,
	}
}

func (eq *EucQueue) Push(id NodeID, dist float64) {
	heap.Push(eq.queue, &Item{
		id:   id,
		dist: dist,
	})
}

func (eq *EucQueue) Pop() *Item {
	if eq.queue.Len() == 0 {
		return nil
	}
	return heap.Pop(eq.queue).(*Item)
}

func (eq *EucQueue) IsEmpty() bool {
	return eq.queue.Len() == 0
}

func (eq *EucQueue) Len() int {
	return eq.queue.Len()
}

// MinQueue is a priority queue where the minimum distance has the highest priority.
// Use case for this is when we need to find the closest neighbor for a given layer.
// MinQueue implements heap.Interface
type MinQueue []*Item

func (pq MinQueue) Len() int { return len(pq) }
func (pq MinQueue) Less(i, j int) bool {
	return pq[i].dist < pq[j].dist
}
func (pq MinQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *MinQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *MinQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *MinQueue) update(item *Item, id NodeID, dist float64) {
	item.id = id
	item.dist = dist
	heap.Fix(pq, item.index)
}

// MaxQueue is a priority queue where the maximum distance has the highest priority.
// Use case for this is when we need to find the furthest neighbor for a given layer.
// MaxQueue implements heap.Interface
type MaxQueue []*Item

func (pq MaxQueue) Len() int { return len(pq) }
func (pq MaxQueue) Less(i, j int) bool {
	return pq[i].dist > pq[j].dist
}
func (pq MaxQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *MaxQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *MaxQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *MaxQueue) update(item *Item, id NodeID, dist float64) {
	item.id = id
	item.dist = dist
	heap.Fix(pq, item.index)
}
