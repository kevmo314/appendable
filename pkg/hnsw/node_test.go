package hnsw

import (
	"math"
	"testing"
)

func TestVec(t *testing.T) {

	type t_case struct {
		u, v     []float64
		expected float64
	}

	bank := [7]t_case{
		{
			u:        []float64{5, 3, 0},
			v:        []float64{2, -2, math.Sqrt(2)},
			expected: 6,
		},
		{
			u:        []float64{1, 0, -5},
			v:        []float64{-3, 2, -1},
			expected: 6,
		},
		{
			u:        []float64{1, 3},
			v:        []float64{5, 2},
			expected: math.Sqrt(17),
		},
		{
			u:        []float64{0, 1, 4},
			v:        []float64{2, 9, 1},
			expected: math.Sqrt(77),
		},
		{
			u:        []float64{0},
			v:        []float64{0},
			expected: 0,
		},
		{
			u:        []float64{10, 20, 30, 40},
			v:        []float64{10, 20, 30, 40},
			expected: math.Sqrt(0),
		},
	}

	t.Run("correctly computes the dist from node", func(t *testing.T) {
		for i, bank := range bank {

			if !NearlyEqual(bank.expected, EuclidDist(bank.u, bank.v)) {
				t.Fatalf("err at %v, expected %v, got %v", i, bank.expected, EuclidDist(bank.u, bank.v))
			}
		}
	})

	t.Run("symmetric", func(t *testing.T) {
		for i, bank := range bank {

			if !NearlyEqual(EuclidDist(bank.v, bank.u), EuclidDist(bank.u, bank.v)) {
				t.Fatalf("err at %v, expected %v, got %v", i, bank.expected, EuclidDist(bank.u, bank.v))
			}
		}
	})
}
