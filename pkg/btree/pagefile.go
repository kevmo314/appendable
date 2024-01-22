package btree

import (
	"encoding/binary"
	"errors"
	"io"
)

type ReadWriteSeekPager interface {
	io.ReadWriteSeeker

	Page(int) (int64, error)
	NewPage() (int64, error)
	FreePage(int64) error

	PageSize() int
}

type PageFile struct {
	io.ReadWriteSeeker
	pageSize int

	// local cache of free pages to avoid reading from disk too often.
	freePageIndexes [512]int64
}

var _ ReadWriteSeekPager = &PageFile{}

const maxFreePageIndices = 512
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
		if _, err := rws.Write(make([]byte, pageSizeBytes)); err != nil {
			return nil, err
		}
	} else {
		for i := 0; i < len(pf.freePageIndexes); i++ {
			pf.freePageIndexes[i] = int64(binary.BigEndian.Uint64(buf[i*8 : (i+1)*8]))
		}
	}
	return pf, nil
}

func (pf *PageFile) Page(i int) (int64, error) {
	if i < 0 {
		return 0, errors.New("page index cannot be negative")
	}
	return int64(i) * int64(pf.pageSize), nil
}

func (pf *PageFile) NewPage() (int64, error) {
	// if there are free pages, return the first one
	for i := 0; i < len(pf.freePageIndexes); i++ {
		if pf.freePageIndexes[i] != 0 {
			offset := pf.freePageIndexes[i]
			// zero out this free page index on disk
			if _, err := pf.ReadWriteSeeker.Seek(int64(i*8), io.SeekStart); err != nil {
				return 0, err
			}
			if _, err := pf.ReadWriteSeeker.Write(make([]byte, 8)); err != nil {
				return 0, err
			}
			// seek to the free page
			if _, err := pf.ReadWriteSeeker.Seek(offset, io.SeekStart); err != nil {
				return 0, err
			}
			return offset, nil
		}
	}

	// seek to the end of the file
	offset, err := pf.ReadWriteSeeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	// if the offset is not a multiple of the page size, we need to pad the file
	// with zeros to the next page boundary.
	if pf.pageSize > 0 && offset%int64(pf.pageSize) != 0 {
		// Calculate the number of bytes to pad
		pad := int64(pf.pageSize) - (offset % int64(pf.pageSize))
		// Write the padding
		if _, err := pf.Write(make([]byte, pad)); err != nil {
			return 0, err
		}
		return offset + pad, nil
	}
	return offset, nil
}

func (pf *PageFile) FreePage(offset int64) error {
	if offset%int64(pf.pageSize) != 0 {
		return errors.New("offset is not a multiple of the page size")
	}
	// find the last nonzero free page index and insert it after that
	for i := len(pf.freePageIndexes) - 1; i >= 0; i-- {
		if pf.freePageIndexes[i] == 0 {
			j := (i + 1) % len(pf.freePageIndexes)
			pf.freePageIndexes[j] = offset

			// write the free page index to the last page
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(offset))
			if _, err := pf.ReadWriteSeeker.Seek(int64(j*8), io.SeekStart); err != nil {
				return err
			}
			return nil
		}
	}
	return errors.New("too many free pages")
}

func (pf *PageFile) PageSize() int {
	return pf.pageSize
}
