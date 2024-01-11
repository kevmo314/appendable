package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func WriteByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}

func WriteUint8(w io.Writer, u uint8) error {
	return binary.Write(w, binary.BigEndian, u)
}

func WriteUint16(w io.Writer, u uint16) error {
	return binary.Write(w, binary.BigEndian, u)
}

func WriteUint32(w io.Writer, u uint32) error {
	return binary.Write(w, binary.BigEndian, u)
}

func WriteUint64(w io.Writer, u uint64) error {
	return binary.Write(w, binary.BigEndian, u)
}

func PackFint16(w io.Writer, i int) error {
	return binary.Write(w, binary.BigEndian, EncodeFloatingInt16(i))
}

func SizeString(s string) int {
	return binary.Size(uint32(len(s))) + len(s)
}

func WriteString(w io.Writer, s string) error {
	if len(s) > math.MaxUint32 {
		return fmt.Errorf("string too long: %d bytes (max %d bytes)", len(s), math.MaxUint32)
	}
	if err := WriteUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func ReadByte(r io.Reader) (byte, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

func ReadUint8(r io.Reader) (uint8, error) {
	var u uint8
	if err := binary.Read(r, binary.BigEndian, &u); err != nil {
		return 0, err
	}
	return u, nil
}

func ReadUint16(r io.Reader) (uint16, error) {
	var u uint16
	if err := binary.Read(r, binary.BigEndian, &u); err != nil {
		return 0, err
	}
	return u, nil
}

func ReadUint32(r io.Reader) (uint32, error) {
	var u uint32
	if err := binary.Read(r, binary.BigEndian, &u); err != nil {
		return 0, err
	}
	return u, nil
}

func ReadUint64(r io.Reader) (uint64, error) {
	var u uint64
	if err := binary.Read(r, binary.BigEndian, &u); err != nil {
		return 0, err
	}
	return u, nil
}

func UnpackFint16(r io.Reader) (int, error) {
	var i FloatingInt16
	if err := binary.Read(r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return DecodeFloatingInt16(i), nil
}

func ReadString(r io.Reader) (string, error) {
	l, err := ReadUint32(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}
