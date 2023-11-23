package encoding

import "testing"

func TestFloatingInt16(t *testing.T) {
	t.Run("DecodeFieldLength is monotonic", func(t *testing.T) {
		prev := -1
		for i := 0; i < 0xFFFF; i++ {
			dec := DecodeFloatingInt16(FloatingInt16(i))
			if dec < prev {
				t.Errorf("DecodeFieldLength(%d) = %d, expected >= %d", i, dec, prev)
			}
			prev = dec
		}
	})

	t.Run("reencoding works", func(t *testing.T) {
		for i := 0; i < 0xFFFF; i++ {
			dec := DecodeFloatingInt16(FloatingInt16(i))
			enc := EncodeFloatingInt16(dec)
			if enc != FloatingInt16(i) {
				t.Errorf("EncodeFieldLength(%d) = %016b, expected %016b", dec, enc, i)
			}
		}
	})

	t.Run("decode max value works", func(t *testing.T) {
		dec := DecodeFloatingInt16(FloatingInt16(0xFFFF))
		if dec != 8793945536512 {
			t.Errorf("DecodeFieldLength(0xFFFF) = %d, expected %d", dec, 8793945536512)
		}
	})
}

func FuzzFloatingInt16(f *testing.F) {
	f.Fuzz(func(t *testing.T, data uint32) {
		enc := EncodeFloatingInt16(int(data))
		dec := DecodeFloatingInt16(enc)
		if dec < int(data) {
			t.Errorf("DecodeFieldLength(EncodeFieldLength(%d)) = %d, expected %d", data, dec, data)
		}
	})
}
