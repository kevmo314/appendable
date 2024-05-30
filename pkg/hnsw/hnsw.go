package hnsw

import (
	"fmt"
)

type Id = uint

type Hnsw struct {
	vectorDimensionality int

	Vectors map[Id]*Vector

	normFactorForLevelGeneration int

	// efConstruction is the size of the dynamic candIdate list
	efConstruction uint

	// default number of connections
	M int

	// mmax, mmax0 is the maximum number of connections for each element per layer
	mmax, mmax0 int
}

func NewHnsw(d int, efConstruction uint, M, mmax, mmax0 int) *Hnsw {
	if d <= 0 {
		panic("vector dimensionality cannot be less than 1")
	}

	return &Hnsw{
		vectorDimensionality: d,
		efConstruction:       efConstruction,
		M:                    M,
		mmax:                 mmax,
		mmax0:                mmax0,
	}
}

func (h *Hnsw) InsertVector(v *Vector) error {
	if !h.validateVector(v) {
		return fmt.Errorf("invalidvector")
	}

	return nil
}

func (h *Hnsw) validateVector(v *Vector) bool {
	return len(v.point) != h.vectorDimensionality
}
