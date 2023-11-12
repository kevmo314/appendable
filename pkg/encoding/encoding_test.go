package encoding

import (
	"bytes"
	"testing"
)

func TestEncoding(t *testing.T) {
	t.Run("byte encoding", func(t *testing.T) {
		b := byte(1)
		buf := &bytes.Buffer{}
		if err := WriteByte(buf, b); err != nil {
			t.Fatal(err)
		}
		b2, err := ReadByte(buf)
		if err != nil {
			t.Fatal(err)
		}
		if b != b2 {
			t.Errorf("expected %v, got %v", b, b2)
		}
	})

	t.Run("uint32 encoding", func(t *testing.T) {
		u := uint32(1)
		buf := &bytes.Buffer{}
		if err := WriteUint32(buf, u); err != nil {
			t.Fatal(err)
		}
		u2, err := ReadUint32(buf)
		if err != nil {
			t.Fatal(err)
		}
		if u != u2 {
			t.Errorf("expected %v, got %v", u, u2)
		}
	})

	t.Run("uint64 encoding", func(t *testing.T) {
		u := uint64(1)
		buf := &bytes.Buffer{}
		if err := WriteUint64(buf, u); err != nil {
			t.Fatal(err)
		}
		u2, err := ReadUint64(buf)
		if err != nil {
			t.Fatal(err)
		}
		if u != u2 {
			t.Errorf("expected %v, got %v", u, u2)
		}
	})

	t.Run("string encoding", func(t *testing.T) {
		s := "test"
		buf := &bytes.Buffer{}
		if err := WriteString(buf, s); err != nil {
			t.Fatal(err)
		}
		s2, err := ReadString(buf)
		if err != nil {
			t.Fatal(err)
		}
		if s != s2 {
			t.Errorf("expected %v, got %v", s, s2)
		}
	})

	t.Run("sha256 hash encoding", func(t *testing.T) {
		h := [32]byte{1}
		buf := &bytes.Buffer{}
		if err := WriteSHA256Hash(buf, h); err != nil {
			t.Fatal(err)
		}
		h2, err := ReadSHA256Hash(buf)
		if err != nil {
			t.Fatal(err)
		}
		if h != h2 {
			t.Errorf("expected %v, got %v", h, h2)
		}
	})
}
