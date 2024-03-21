package metapage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
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

func (m *MultiPager) Root(offset uint64) (btree.MemoryPointer, error) {
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return btree.MemoryPointer{}, err
	}

	var mp btree.MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *MultiPager) SetRoot(offset uint64, mp btree.MemoryPointer) error {
	if _, err := m.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	return binary.Write(m.rws, binary.LittleEndian, mp)
}

func (m *MultiPager) Metadata(offset uint64) ([]byte, error) {
	if _, err := m.rws.Seek(int64(offset)+(8*N+16), io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, m.rws.PageSize()-(8*N+16))
	if _, err := m.rws.Read(buf); err != nil {
		return nil, err
	}
	// the first four bytes represents the length
	length := binary.LittleEndian.Uint32(buf[:4])
	return buf[4 : 4+length], nil
}

func (m *MultiPager) SetMetadata(offset uint64, data []byte) error {
	if len(data) > m.rws.PageSize()-(8*N+16) {
		return errors.New("metadata too large")
	}
	if _, err := m.rws.Seek(int64(offset)+(8*N+16), io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := m.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (m *MultiPager) NextNOffsets(offset uint64, offsets []uint64) ([]uint64, error) {
	if _, err := m.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}

	for i := 0; i < N; i++ {
		if err := binary.Read(m.rws, binary.LittleEndian, &offsets[i]); err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

func (m *MultiPager) SetNextNOffsets(offset uint64, offsets []uint64) error {
	if len(offsets) > N {
		return fmt.Errorf("too many offsets, max number of offsets should be %d", N)
	}

	if _, err := m.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return err
	}

	for _, offset := range offsets {
		if err := binary.Write(m.rws, binary.LittleEndian, offset); err != nil {
			return err
		}
	}

	if err := binary.Write(m.rws, binary.LittleEndian, ^uint64(0)); err != nil {
		return err
	}
	return nil
}

func (m *MultiPager) Next(offset uint64) (*LinkedMetaSlot, error) {
	if _, err := m.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	var next btree.MemoryPointer
	if err := binary.Read(m.rws, binary.LittleEndian, &next); err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{rws: m.rws, offset: next.Offset, pager: m}, nil
}

func (m *MultiPager) NextSlot(buf []byte) (int64, error) {
	if buf != nil && len(buf) > m.rws.SlotSize() {
		return 0, errors.New("buffer is too large")
	}

	// find next available page offset
	for pageIndex, metaIndexUsed := range m.freeSlotIndexes {
		for slotIndex, used := range metaIndexUsed {
			if !used {
				m.freeSlotIndexes[pageIndex][slotIndex] = true
				offset := int64(pageIndex*m.rws.PageSize()) + int64(slotIndex*m.rws.SlotSize())
				fmt.Printf("page index: %v, slot index: %v, offset: %v\n", pageIndex, slotIndex, offset)
				return offset, nil
			}
		}
	}

	newPageOffset, err := m.rws.NewPage(nil)
	if err != nil {
		return 0, err
	}

	pageIndex := newPageOffset / int64(m.rws.PageSize())
	metaSlotsPerPage := m.rws.PageSize() / m.rws.SlotSize()
	metaSlotsRow := make([]bool, metaSlotsPerPage)
	metaSlotsRow[0] = true
	m.freeSlotIndexes = append(m.freeSlotIndexes, metaSlotsRow)

	return pageIndex * int64(m.rws.PageSize()), nil

}

func (m *MultiPager) AddNext(offset uint64) (*LinkedMetaSlot, error) {
	curr, err := m.Next(offset)
	if err != nil {
		return nil, err
	}
	if curr.offset != ^uint64(0) {
		return nil, errors.New("next pointer already exists")
	}

	nextOffset, err := m.NextSlot(nil)
	if err != nil {
		return nil, err
	}
	next := &LinkedMetaSlot{rws: m.rws, offset: uint64(nextOffset), pager: m}
	fmt.Printf("next offset: %v\n", nextOffset)
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
