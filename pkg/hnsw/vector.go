package hnsw

import (
	"errors"
	"math"
)

type Point []float32

type Vector struct {
	id    Id
	point Point

	friends []*BaseQueue
}

// NewVector creates a new vector, note the max level is inclusive.
func NewVector(id Id, point Point, maxLevel int) *Vector {
	friends := make([]*BaseQueue, maxLevel+1)

	for i := 0; i <= maxLevel; i++ {
		friends[i] = NewBaseQueue(MinComparator{})
	}

	return &Vector{
		id:      id,
		point:   point,
		friends: friends,
	}
}

func (v *Vector) Levels() int {
	return len(v.friends)
}

func (v *Vector) MaxLevel() int {
	return len(v.friends) - 1
}

func (v *Vector) HasLevel(level int) bool {
	if level < 0 {
		panic("level must be nonzero positive integer")
	}

	return level <= v.MaxLevel()
}

// InsertFriendsAtLevel requires level must be zero-indexed
func (v *Vector) InsertFriendsAtLevel(level int, friend *Vector) {
	if !v.HasLevel(level) {
		panic("failed to insert friends at level, as level is not valId")
	}

	if friend.id == v.id {
		panic("cannot insert yourself to friends list")
	}

	dist := v.EuclidDistance(friend)

	for i := 0; i <= level; i++ {
		v.friends[i].Insert(friend.id, dist)
	}
}

func (v *Vector) GetFriendsAtLevel(level int) (*BaseQueue, error) {
	if !v.HasLevel(level) {
		return nil, errors.New("failed to get friends at level")
	}

	return v.friends[level], nil
}

func (v *Vector) EuclidDistance(v1 *Vector) float32 {
	return v.EuclidDistanceFromPoint(v1.point)
}

func (v *Vector) EuclidDistanceFromPoint(point Point) float32 {
	var sum float32

	for i := range v.point {
		delta := v.point[i] - point[i]
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
