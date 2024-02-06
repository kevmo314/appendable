// mmap contains utilities to memory map a file while still exposing file append operations.
package mmap

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

type MemoryMappedFile struct {
	file  *os.File
	bytes []byte
	seek  int64
}

var _ io.ReadWriteSeeker = &MemoryMappedFile{}
var _ io.Closer = &MemoryMappedFile{}
var _ io.ReaderAt = &MemoryMappedFile{}
var _ io.WriterAt = &MemoryMappedFile{}

func NewMemoryMappedFile(f *os.File) (*MemoryMappedFile, error) {
	fd := uintptr(f.Fd())
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %v", err)
	}
	if fi.Size() == 0 {
		return &MemoryMappedFile{file: f, bytes: nil, seek: 0}, nil
	}
	b, err := unix.Mmap(int(fd), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %v", err)
	}
	return &MemoryMappedFile{file: f, bytes: b, seek: 0}, nil
}

// Close closes the file and unmaps the memory.
func (m *MemoryMappedFile) Close() error {
	if m.bytes == nil {
		return nil
	}
	return unix.Munmap(m.bytes)
}

// Seek sets the offset for the next Read or Write on file to offset.
func (m *MemoryMappedFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = m.seek + offset
	case io.SeekEnd:
		abs = int64(len(m.bytes)) + offset
	default:
		return 0, fmt.Errorf("mmap: invalid whence")
	}
	if abs < 0 {
		return 0, fmt.Errorf("mmap: negative position")
	} else if abs > int64(len(m.bytes)) {
		return 0, fmt.Errorf("mmap: position out of bounds")
	}
	m.seek = abs
	return abs, nil
}

// Read reads up to len(b) bytes from the file.
func (m *MemoryMappedFile) Read(b []byte) (int, error) {
	n := copy(b, m.bytes[m.seek:])
	m.seek += int64(n)
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// ReadAt reads len(b) bytes from the file starting at byte offset off.
func (m *MemoryMappedFile) ReadAt(b []byte, off int64) (int, error) {
	n := copy(b, m.bytes[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// Write writes len(b) bytes to the file, appending to the file and remapping if necessary.
func (m *MemoryMappedFile) Write(b []byte) (int, error) {
	n, err := m.WriteAt(b, m.seek)
	if err != nil {
		return 0, err
	}
	m.seek += int64(n)
	return n, nil
}

// WriteAt writes len(b) bytes to the file starting at byte offset off.
func (m *MemoryMappedFile) WriteAt(b []byte, off int64) (int, error) {
	// check if the file needs to be remapped
	if off+int64(len(b)) > int64(len(m.bytes)) {
		// write the data and remap the file
		if _, err := m.file.WriteAt(b, off); err != nil {
			return 0, err
		}
		fi, err := m.file.Stat()
		if err != nil {
			return 0, err
		}
		if m.bytes == nil {
			m.bytes, err = unix.Mmap(int(m.file.Fd()), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				return 0, fmt.Errorf("mmap: %v", err)
			}
			return len(b), nil
		}
		b, err := unix.Mremap(m.bytes, int(fi.Size()), unix.MREMAP_MAYMOVE)
		if err != nil {
			return 0, fmt.Errorf("mmap: %v", err)
		}
		m.bytes = b
		return len(b), nil
	}
	// write the data
	n := copy(m.bytes[off:], b)
	return n, nil
}
