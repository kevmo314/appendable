package btree

import "io"

type PageFile struct {
	io.ReadWriteSeeker
	PageSize int
}

func (pf *PageFile) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekEnd {
		// Seek to the end of the file
		offset, err := pf.ReadWriteSeeker.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		// If the offset is not a multiple of the page size, we need to pad the file
		// with zeros to the next page boundary.
		if pf.PageSize > 0 && offset%int64(pf.PageSize) != 0 {
			// Calculate the number of bytes to pad
			pad := int64(pf.PageSize) - (offset % int64(pf.PageSize))
			// Write the padding
			if _, err := pf.Write(make([]byte, pad)); err != nil {
				return 0, err
			}
			return offset + pad, nil
		}
		return offset, nil
	}
	return pf.ReadWriteSeeker.Seek(offset, whence)
}
