//go:build !release

package pagefile

import "io"

func (pf *PageFile) Write(buf []byte) (int, error) {
	n, err := pf.ReadWriteSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if n%int64(pf.pageSize)+int64(len(buf)) > int64(pf.pageSize) {
		panic("writing across page boundary not allowed")
	}
	return pf.ReadWriteSeeker.Write(buf)
}

func (pf *PageFile) Read(buf []byte) (int, error) {
	n, err := pf.ReadWriteSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if n%int64(pf.pageSize)+int64(len(buf)) > int64(pf.pageSize) {
		panic("reading across page boundary not allowed")
	}
	return pf.ReadWriteSeeker.Read(buf)
}
