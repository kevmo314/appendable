package pagefile

import (
	"encoding"
	"io"
)

type ObjectReadWriterAt interface {
	ReadAt(p encoding.BinaryUnmarshaler, off int64) (n int, err error)
	WriteAt(p encoding.BinaryMarshaler, off int64) (n int, err error)
}

type BufferObjectReadWriterAt struct {
	rws   io.ReadWriteSeeker
	cache map[int64]any
}

func NewBufferObjectReadWriterAt(rws io.ReadWriteSeeker) *BufferObjectReadWriterAt {
	return &BufferObjectReadWriterAt{rws: rws}
}

func (b *BufferObjectReadWriterAt) ReadAt(p io.ReaderFrom, off int64) (v any, err error) {
	if v, ok := b.cache[off]; ok {
		return v, nil
	}

	if _, err := b.rws.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	p.ReadFrom(b.rws)
	b.cache[off] = p
	return p, nil
}
