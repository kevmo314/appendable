package btree

import (
	"encoding/binary"
	"errors"
	"io"
)

type LinkedMetaPage struct {
	rws    ReadWriteSeekPager
	offset uint64
}

func (m *LinkedMetaPage) Root() (MemoryPointer, error) {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return MemoryPointer{}, err
	}
	var mp MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *LinkedMetaPage) SetRoot(mp MemoryPointer) error {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return err
	}
	return binary.Write(m.rws, binary.LittleEndian, mp)
}

func (m *LinkedMetaPage) Metadata() ([]byte, error) {
	if _, err := m.rws.Seek(int64(m.offset)+24, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, m.rws.PageSize()-24)
	if _, err := m.rws.Read(buf); err != nil {
		return nil, err
	}
	// the first four bytes represents the length
	length := binary.LittleEndian.Uint32(buf[:4])
	return buf[4 : 4+length], nil
}

func (m *LinkedMetaPage) SetMetadata(data []byte) error {
	if len(data) > m.rws.PageSize()-24 {
		return errors.New("metadata too large")
	}
	if _, err := m.rws.Seek(int64(m.offset)+24, io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := m.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (m *LinkedMetaPage) Next() (*LinkedMetaPage, error) {
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	var next MemoryPointer
	if err := binary.Read(m.rws, binary.LittleEndian, &next); err != nil {
		return nil, err
	}
	if next.Offset == 0 {
		return nil, nil
	}
	return &LinkedMetaPage{rws: m.rws, offset: next.Offset}, nil
}

func (m *LinkedMetaPage) AddNext() (*LinkedMetaPage, error) {
	// check that the next pointer is zero
	curr, err := m.Next()
	if err != nil {
		return nil, err
	}
	if curr != nil {
		return nil, errors.New("next pointer is not zero")
	}
	offset, err := m.rws.NewPage()
	if err != nil {
		return nil, err
	}
	next := &LinkedMetaPage{rws: m.rws, offset: uint64(offset)}
	if err := next.Reset(); err != nil {
		return nil, err
	}
	// save the next pointer
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	if err := binary.Write(m.rws, binary.LittleEndian, next.offset); err != nil {
		return nil, err
	}
	return next, nil
}

func (m *LinkedMetaPage) MemoryPointer() MemoryPointer {
	return MemoryPointer{Offset: m.offset, Length: 24}
}

func (m *LinkedMetaPage) Exists() (bool, error) {
	length, err := m.rws.Seek(0, io.SeekEnd)
	if err != nil {
		return false, err
	}
	return length > int64(m.offset), nil
}

func (m *LinkedMetaPage) Reset() error {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return err
	}
	// write 28 bytes of zeros
	if _, err := m.rws.Write(make([]byte, 28)); err != nil {
		return err
	}
	return nil
}

func NewMultiBPTree(t ReadWriteSeekPager, offset uint64) *LinkedMetaPage {
	return &LinkedMetaPage{rws: t, offset: offset}
}
