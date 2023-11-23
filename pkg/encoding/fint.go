package encoding

// FloatingInt16 is a 16-bit floating point number that represents a field length.
//
// This encoding format encodes integers in the range [0, 8793945536512] with 16 bits.
//
// The encoding is (2^exponent) * mantissa
//
// mantissa has range [0, 2^11), exponent has range [0, 2^5)
type FloatingInt16 uint16

func EncodeFloatingInt16(length int) FloatingInt16 {
	// convert this to a the next largest floating point
	exponent := 0
	chunkSize := 1 << 11
	for length >= chunkSize {
		exponent++
		length -= chunkSize
		chunkSize <<= 1
	}
	// length is in this exponent's chunk, round it up to the nearest 2^exponent
	length = (length + (1 << exponent) - 1) >> exponent
	return FloatingInt16((exponent << 11) | length)
}

func DecodeFloatingInt16(length FloatingInt16) int {
	exponent := int(length >> 11)
	mantissa := int(length & 0x7FF)
	return (1<<exponent)*mantissa + (1 << (exponent + 11)) - (1 << 11)
}
