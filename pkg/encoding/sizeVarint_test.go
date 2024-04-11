package encoding

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

func TestSizeVariant(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	const iterations = 1000

	for i := 0; i < iterations; i++ {
		randomNumber := rand.Uint64()

		x := len(binary.AppendUvarint([]byte{}, randomNumber))
		y := SizeVarint(randomNumber)

		if x != y {
			t.Fatalf("Mismatch for %d: binary.AppendUvarint size = %d, SizeVarint size = %d", randomNumber, x, y)
		}
	}
}
