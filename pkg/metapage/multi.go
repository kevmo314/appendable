package metapage

import (
	"encoding/binary"
	"errors"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
)

type MultiPager struct {
	rws             pagefile.ReadWriteSeekPager
	freeSlotIndexes [][]bool
}

func New(t pagefile.ReadWriteSeekPager) *MultiPager {
	metaSlotsPerPage := t.PageSize() / t.SlotSize()

	is := make([][]bool, t.LastPage())

	for i := range is {
		is[i] = make([]bool, metaSlotsPerPage)
	}

	m := &MultiPager{
		rws:             t,
		freeSlotIndexes: is,
	}

	return m
}

func (m *MultiPager) Root(offset uint64) (pointer.MemoryPointer, error) {
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return pointer.MemoryPointer{}, err
	}

	var mp pointer.MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *MultiPager) SetRoot(offset uint64, mp pointer.MemoryPointer) error {
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	return binary.Write(m.rws, binary.LittleEndian, mp)
}

func (m *MultiPager) Metadata(offset uint64) ([]byte, error) {
	if _, err := m.rws.Seek(int64(offset)+24, io.SeekStart); err != nil {
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

func (m *MultiPager) SetMetadata(offset uint64, data []byte) error {
	if len(data) > m.rws.PageSize()-24 {
		return errors.New("metadata too large")
	}
	if _, err := m.rws.Seek(int64(offset)+24, io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := m.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (m *MultiPager) Next(offset uint64) (*LinkedMetaSlot, error) {
	if _, err := m.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	var next pointer.MemoryPointer
	if err := binary.Read(m.rws, binary.LittleEndian, &next); err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{offset: next.Offset, pager: m}, nil
}

func (m *MultiPager) GetNextSlot(buf []byte) (int64, error) {
	if buf != nil && len(buf) > m.rws.SlotSize() {
		return 0, errors.New("buffer is too large")
	}

	// find next available page offset
	for pageIndex, slots := range m.freeSlotIndexes {
		for slotIndex, used := range slots {
			if !used {
				m.freeSlotIndexes[pageIndex][slotIndex] = true
				offset := int64(pageIndex*m.rws.PageSize() + slotIndex*m.rws.SlotSize())
				return offset, nil
			}
		}
	}

	newPageOffset, err := m.rws.NewPage(nil)
	if err != nil {
		return 0, err
	}

	pageIndex := int(newPageOffset / int64(m.rws.PageSize()))
	if pageIndex >= len(m.freeSlotIndexes) {
		for len(m.freeSlotIndexes) <= pageIndex {
			m.freeSlotIndexes = append(m.freeSlotIndexes, make([]bool, m.rws.PageSize()/m.rws.SlotSize()))
		}
	}

	m.freeSlotIndexes[pageIndex][0] = true
	return newPageOffset, nil
}

func (m *MultiPager) AddNext(offset uint64) (*LinkedMetaSlot, error) {
	exists, err := m.Next(offset)
	if err != nil {
		return nil, err
	}
	if exists.offset != ^uint64(0) {
		return nil, errors.New("next pointer already exists")
	}

	nextOffset, err := m.GetNextSlot(nil)
	if err != nil {
		return nil, err
	}
	next := &LinkedMetaSlot{offset: uint64(nextOffset), pager: m}
	if err := next.Reset(); err != nil {
		return nil, err
	}
	// save the next pointer
	if _, err := m.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	if err := binary.Write(m.rws, binary.LittleEndian, next.offset); err != nil {
		return nil, err
	}
	return next, nil
}

func (m *MultiPager) Exists(offset uint64) (bool, error) {
	if offset == ^uint64(0) {
		return false, nil
	}
	// attempt to read the page
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return false, err
	}
	if _, err := m.rws.Read(make([]byte, m.rws.PageSize())); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *MultiPager) Reset(offset uint64) error {
	// write a full page of zeros
	emptyPage := make([]byte, m.rws.PageSize())
	binary.LittleEndian.PutUint64(emptyPage[12:20], ^uint64(0))
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	if _, err := m.rws.Write(emptyPage); err != nil {
		return err
	}
	return nil
}

func (m *MultiPager) PageCount() int64 {
	return m.rws.PageCount()
}
