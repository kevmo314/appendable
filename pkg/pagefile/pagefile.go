package pagefile

import (
	"encoding/binary"
	"errors"
	"io"
)

type ReadWriteSeekPager interface {
	io.ReadWriteSeeker

	Page(int) (int64, error)
	NewPage([]byte) (int64, error)
	FreePage(int64) error

	PageSize() int
}

type PageFile struct {
	io.ReadWriteSeeker
	pageSize int

	// local cache of free pages to avoid reading from disk too often.
	freePageIndexes             [512]int64
	freePageHead, freePageCount int

	lastPage int64
}

var _ ReadWriteSeekPager = &PageFile{}

// const maxFreePageIndices = 512
const pageSizeBytes = 4096 // 4kB by default.

func NewPageFile(rws io.ReadWriteSeeker) (*PageFile, error) {
	// check if the rws is empty. if it is, allocate one page for the free page indexes
	// if it is not, read the free page indexes from the last page
	if _, err := rws.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, pageSizeBytes)
	_, err := rws.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	pf := &PageFile{
		ReadWriteSeeker: rws,
		pageSize:        pageSizeBytes,
	}
	if err == io.EOF {
		// allocate one page for the free page indexes
		if _, err := rws.Write(buf); err != nil {
			return nil, err
		}
	} else {
		for i := 0; i < len(pf.freePageIndexes); i++ {
			offset := int64(binary.LittleEndian.Uint64(buf[i*8 : (i+1)*8]))
			if offset != 0 {
				pf.freePageIndexes[pf.freePageHead] = offset
				pf.freePageHead = (pf.freePageHead + 1) % len(pf.freePageIndexes)
				pf.freePageCount++
			} else {
				break
			}
		}
	}
	// figure out what the last page is
	n, err := rws.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if n%int64(pf.pageSize) != 0 {
		return nil, errors.New("file size is not a multiple of the page size")
	}
	pf.lastPage = n / int64(pf.pageSize)
	return pf, nil
}

func (pf *PageFile) Page(i int) (int64, error) {
	if i < 0 {
		return 0, errors.New("page index cannot be negative")
	}
	// i + 1 because the first page is reserved for the free page indexes
	return int64(i+1) * int64(pf.pageSize), nil
}

func (pf *PageFile) writeFreePageIndices() error {
	buf := make([]byte, len(pf.freePageIndexes)*8)
	tail := (pf.freePageHead - pf.freePageCount + len(pf.freePageIndexes)) % len(pf.freePageIndexes)
	for i := 0; i < pf.freePageCount; i++ {
		offset := pf.freePageIndexes[tail+i]
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], uint64(offset))
	}
	if _, err := pf.ReadWriteSeeker.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := pf.ReadWriteSeeker.Write(buf); err != nil {
		return err
	}
	return nil
}

func (pf *PageFile) FreePageIndex() (int64, error) {
	// find the first free page index and return it
	if pf.freePageCount == 0 {
		return -1, nil
	}
	// pop from the tail
	tail := (pf.freePageHead - pf.freePageCount + len(pf.freePageIndexes)) % len(pf.freePageIndexes)
	offset := pf.freePageIndexes[tail]
	pf.freePageIndexes[tail] = 0
	pf.freePageCount--

	if err := pf.writeFreePageIndices(); err != nil {
		return 0, err
	}

	return offset, nil
}

func (pf *PageFile) NewPage(buf []byte) (int64, error) {
	if buf != nil && len(buf) > pf.pageSize {
		return 0, errors.New("buffer is too large")
	}

	// if there are free pages, return the first one
	offset, err := pf.FreePageIndex()
	if err != nil {
		return 0, err
	}
	if offset != -1 {
		// seek to the free page
		if _, err := pf.ReadWriteSeeker.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
	} else {
		n, err := pf.ReadWriteSeeker.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		offset = n
		pf.lastPage++
	}

	// if the offset is not a multiple of the page size, we need to pad the file
	// with zeros to the next page boundary.
	var pad int64
	if pf.pageSize > 0 && offset%int64(pf.pageSize) != 0 {
		// Calculate the number of bytes to pad
		pad = int64(pf.pageSize) - (offset % int64(pf.pageSize))
		// Write the padding
		if _, err := pf.Write(make([]byte, pad)); err != nil {
			return 0, err
		}
	}
	page := make([]byte, pf.pageSize)
	if buf != nil {
		copy(page, buf)
	}
	if _, err := pf.ReadWriteSeeker.Write(page); err != nil {
		return 0, err
	}
	if _, err := pf.ReadWriteSeeker.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return offset + pad, nil
}

func (pf *PageFile) FreePage(offset int64) error {
	if offset%int64(pf.pageSize) != 0 {
		return errors.New("offset is not a multiple of the page size")
	}
	if pf.freePageCount == len(pf.freePageIndexes) {
		return errors.New("free page index is full")
	}

	for i := range pf.freePageIndexes {
		if pf.freePageIndexes[i] == offset {
			return errors.New("offset already exists")
		}
	}

	// push to the head
	pf.freePageIndexes[pf.freePageHead] = offset
	pf.freePageHead = (pf.freePageHead + 1) % len(pf.freePageIndexes)
	pf.freePageCount++

	return pf.writeFreePageIndices()
}

func (pf *PageFile) PageSize() int {
	return pf.pageSize
}

func (pf *PageFile) PageCount() int64 {
	return pf.lastPage
}
