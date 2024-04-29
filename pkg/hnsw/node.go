package hnsw

type Node struct {
	level int
	point *Point

	// fl is the friends list
	fl []*Node
}
