package hnsw

import "fmt"

type Set struct {
	items map[NodeID]interface{}
	size  uint
}

func NewSet() *Set {
	s := Set{
		items: make(map[NodeID]interface{}),
		size:  0,
	}

	return &s
}

func (s *Set) Add(id NodeID) {
	s.items[id] = struct{}{}
}

func (s *Set) Contains(id NodeID) bool {
	_, found := s.items[id]

	return found
}

func (s *Set) Remove(id NodeID) error {
	if _, found := s.items[id]; !found {
		return fmt.Errorf("failed to remove id: %v because it doesn't exist in the set", id)
	}

	delete(s.items, id)
	s.size--

	return nil
}

func (s *Set) Size() int {
	return int(s.size)
}
