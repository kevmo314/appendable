package encoding

import "math/bits"

func SizeVarint(v uint64) int {
	return int(9*uint32(bits.Len64(v))+64) / 64
}
