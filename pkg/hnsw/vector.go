package hnsw

type Point struct {
	rank   int32
	vector Vector
}
type Vector = []float64
