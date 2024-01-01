package btree

import (
	"io"
	"testing"
)

func TestSeekableBuffer(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		b := newSeekableBuffer()
		n, err := b.Write([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 bytes, wrote %d", n)
		}
		if string(b.buf) != "hello" {
			t.Fatalf("expected to write 'hello', wrote %s", string(b.buf))
		}
	})

	t.Run("write to end", func(t *testing.T) {
		b := newSeekableBuffer()
		if _, err := b.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Seek(-2, io.SeekEnd); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Write([]byte("world")); err != nil {
			t.Fatal(err)
		}
		if string(b.buf) != "helworld" {
			t.Fatalf("expected to write 'helworld', wrote %s", string(b.buf))
		}
	})

	t.Run("Seek", func(t *testing.T) {
		b := newSeekableBuffer()
		if _, err := b.Write([]byte("helloo")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Write([]byte("world")); err != nil {
			t.Fatal(err)
		}
		if string(b.buf) != "worldo" {
			t.Fatalf("expected to write 'worldo', wrote %s", string(b.buf))
		}
	})

	t.Run("Read", func(t *testing.T) {
		b := newSeekableBuffer()
		if _, err := b.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 5)
		n, err := b.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to read 5 bytes, read %d", n)
		}
		if string(buf) != "hello" {
			t.Fatalf("expected to read 'hello', read %s", string(buf))
		}
	})

	t.Run("read from middle", func(t *testing.T) {
		b := newSeekableBuffer()
		if _, err := b.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Seek(2, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 3)
		n, err := b.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected to read 3 bytes, read %d", n)
		}
		if string(buf) != "llo" {
			t.Fatalf("expected to read 'llo', read %s", string(buf))
		}
	})

	t.Run("truncate", func(t *testing.T) {
		b := newSeekableBuffer()
		if _, err := b.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if err := b.Truncate(3); err != nil {
			t.Fatal(err)
		}
		if string(b.buf) != "hel" {
			t.Fatalf("expected to truncate to 'hel', truncated to %s", string(b.buf))
		}
	})
}
