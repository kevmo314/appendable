package hnsw

import "math"

/*
The greedy algorithm can be divided into two phases: zoom-out and zoom-in.
Starts in the zoom-out phase from a low degree node, traverses the graph increasing the node's degree.
Halts when characteristic radius of the node links length reaches the scale of the distance to the query.
*/

type Hnsw struct {
	// vector dimensionality
	d int

	// number of neighbors for each vertex
	m int

	// mMax, mMax, default values are set to M and M*2
	mMax, mMax0 int

	maxLevel int

	levels [][]*Node

	// probability of insertion at a given layer
	probas []float64

	// cumulative total of nearest neighbors assigned to a vertex at a insertion level i
	cumNNperLevel []int

	// fixed entry point for insertion + searching
	entryPoint *Node

	// ml (level multiplier) determines the likelihood of a node appearing in successive layers,
	// with the probability decreasing exponentially as the layers ascend. An optimal value is 1 / ln(M)
	levelMult float64
}

// New needs two things: vector dimensionality d
// and m the number of neighbors for each vertex
func New(d, m int) *Hnsw {

	h := &Hnsw{
		d:          d,
		m:          m,
		mMax:       m,
		mMax0:      m * 2,
		maxLevel:   -1,
		levels:     [][]*Node{},
		entryPoint: nil,
		levelMult:  1 / math.Log(float64(m)),
	}

	return h
}

func (h *Hnsw) setDefaultProbas() {
	// setting the nearest neighbors count = 0
	nn := 0
	var cumNNperLevel []int

	maxLevel := 0
	var probas []float64

	for {
		proba := math.Exp(-float64(maxLevel)/h.levelMult) * (1 - math.Exp(-1/h.levelMult))

		if proba < 1e-9 {
			break
		}

		probas = append(probas, proba)

		if maxLevel == 0 {
			nn += h.mMax0
		} else {
			nn += h.mMax
		}

		cumNNperLevel = append(cumNNperLevel, nn)
		maxLevel++
	}

	h.maxLevel = maxLevel
	h.probas = probas
	h.cumNNperLevel = cumNNperLevel
}
