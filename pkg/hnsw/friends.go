package hnsw

import (
	"errors"
	"math"
)

type Point []float32

type Friends struct {
	friends   []*DistHeap
	maxLevels map[Id]int
}

// NewFriends creates a new vector, note the max level is inclusive.
func NewFriends(topLevel int) *Friends {
	friends := make([]*DistHeap, topLevel+1)

	for i := 0; i <= topLevel; i++ {
		friends[i] = NewDistHeap()
	}

	return &Friends{
		friends:   friends,
		maxLevels: make(map[Id]int),
	}
}

func (v *Friends) NumLevels() int {
	return len(v.friends)
}

func (v *Friends) TopLevel() int {
	return len(v.friends) - 1
}

func (v *Friends) HasLevel(level int) bool {
	if level < 0 {
		panic("level must be nonzero positive integer")
	}

	return level <= v.TopLevel()
}

// InsertFriendsAtLevel requires level must be zero-indexed and friendId must be valid at this level
func (v *Friends) InsertFriendsAtLevel(level int, friendId Id, dist float32) {
	if !v.HasLevel(level) {
		panic("failed to insert friends at level, as level is not valId")
	}

	for i := 0; i <= level; i++ {
		v.friends[i].Insert(friendId, dist)
	}

	v.maxLevels[friendId] = level
}

func (v *Friends) GetFriendsAtLevel(level int) (*DistHeap, error) {
	if !v.HasLevel(level) {
		return nil, errors.New("failed to get friends at level")
	}

	return v.friends[level], nil
}

func EuclidDistance(p0, p1 Point) float32 {
	var sum float32

	for i := range p0 {
		delta := p0[i] - p1[i]
		sum += delta * delta
	}

	return float32(math.Sqrt(float64(sum)))
}

// NearlyEqual is sourced from scalar package written by gonum
// https://pkg.go.dev/gonum.org/v1/gonum/floats/scalar#EqualWithinAbsOrRel
func NearlyEqual(a, b float32) bool {
	return EqualWithinAbs(float64(a), float64(b)) || EqualWithinRel(float64(a), float64(b))
}

// EqualWithinAbs returns true when a and b have an absolute difference
// not greater than tol.
func EqualWithinAbs(a, b float64) bool {
	return a == b || math.Abs(a-b) <= 1e-6
}

// minNormalFloat64 is the smallest normal number. For 64 bit IEEE-754
// floats this is 2^{-1022}.
const minNormalFloat64 = 0x1p-1022

// EqualWithinRel returns true when the difference between a and b
// is not greater than tol times the greater absolute value of a and b,
//
//	abs(a-b) <= tol * max(abs(a), abs(b)).
func EqualWithinRel(a, b float64) bool {
	if a == b {
		return true
	}
	delta := math.Abs(a - b)
	if delta <= minNormalFloat64 {
		return delta <= 1e-6*minNormalFloat64
	}
	// We depend on the division in this relationship to Identify
	// infinities (we rely on the NaN to fail the test) otherwise
	// we compare Infs of the same sign and evaluate Infs as equal
	// independent of sign.
	return delta/math.Max(math.Abs(a), math.Abs(b)) <= 1e-6
}
