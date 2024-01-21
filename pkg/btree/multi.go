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

func (m *LinkedMetaPage) Metadata() (MemoryPointer, error) {
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return MemoryPointer{}, err
	}
	var mp MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *LinkedMetaPage) SetMetadata(mp MemoryPointer) error {
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return err
	}
	return binary.Write(m.rws, binary.LittleEndian, mp)
}

func (m *LinkedMetaPage) Next() (*LinkedMetaPage, error) {
	if _, err := m.rws.Seek(int64(m.offset)+24, io.SeekStart); err != nil {
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
	if _, err := m.rws.Seek(int64(m.offset)+24, io.SeekStart); err != nil {
		return nil, err
	}
	if err := binary.Write(m.rws, binary.LittleEndian, next.offset); err != nil {
		return nil, err
	}
	return next, nil
}

func (m *LinkedMetaPage) MemoryPointer() MemoryPointer {
	return MemoryPointer{Offset: m.offset, Length: 36}
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
	// write 36 bytes of zeros
	if _, err := m.rws.Write(make([]byte, 36)); err != nil {
		return err
	}
	return nil
}

func NewMultiBPTree(t ReadWriteSeekPager) (*LinkedMetaPage, error) {
	offset, err := t.NewPage()
	if err != nil {
		return nil, err
	}
	return &LinkedMetaPage{rws: t, offset: uint64(offset)}, nil
}
