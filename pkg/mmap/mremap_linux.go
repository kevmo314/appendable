package mmap

import "golang.org/x/sys/unix"

func mremap(oldAddress []byte, fd, newSize, prot, flags int) ([]byte, error) {
	return unix.Mremap(oldAddress, newSize, unix.MREMAP_MAYMOVE)
}
