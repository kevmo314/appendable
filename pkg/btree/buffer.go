package btree

import "io"

// seekableBuffer is a buffer that can be seeked into.
// this replicates the behavior of a file on disk without having to write to disk
// which is useful for testing.
type seekableBuffer struct {
	buf []byte
	pos int
}

func newSeekableBuffer() *seekableBuffer {
	return &seekableBuffer{}
}

func (b *seekableBuffer) Write(p []byte) (int, error) {
	n := copy(b.buf[b.pos:], p)
	if n < len(p) {
		b.buf = append(b.buf, p[n:]...)
	}
	b.pos += len(p)
	return len(p), nil
}

func (b *seekableBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = int(offset)
	case io.SeekCurrent:
		b.pos += int(offset)
	case io.SeekEnd:
		b.pos = len(b.buf) + int(offset)
	}
	if b.pos < 0 {
		b.pos = 0
	}
	if b.pos > len(b.buf) {
		b.pos = len(b.buf)
	}
	return int64(b.pos), nil
}

func (b *seekableBuffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.buf) {
		return 0, io.EOF
	}
	n := copy(p, b.buf[b.pos:])
	b.pos += n
	return n, nil
}

func (b *seekableBuffer) Truncate(size int64) error {
	if size < 0 {
		return io.ErrShortBuffer
	}
	if size > int64(len(b.buf)) {
		return io.ErrShortWrite
	}
	b.buf = b.buf[:size]
	return nil
}

var _ io.ReadWriteSeeker = &seekableBuffer{}
