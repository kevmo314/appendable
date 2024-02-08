package mmap

import "golang.org/x/sys/unix"

func mremap(oldAddress []byte, fd, newSize, prot, flags int) ([]byte, error) {
	// darwin doesn't have mremap, so we have to munmap and mmap the new size

	// unmap the old address
	if err := unix.Munmap(oldAddress); err != nil {
		return nil, err
	}
	return unix.Mmap(fd, 0, newSize, prot, flags)
}
