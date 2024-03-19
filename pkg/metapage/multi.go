package metapage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"io"
)

// MetaPager manages LinkedMetaSlots

const pageSizeBytes = 4096
const metaSizeBytes = 256

type MetaPager struct {
	rws             pagefile.ReadWriteSeekPager
	freeMetaIndexes [][]bool
}

func (p *MetaPager) SlotRoot(offset uint64) (btree.MemoryPointer, error) {
	if _, err := p.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return btree.MemoryPointer{}, err
	}
	var mp btree.MemoryPointer
	return mp, binary.Read(p.rws, binary.LittleEndian, &mp)
}

func (p *MetaPager) SetSlotRoot(offset uint64, mp btree.MemoryPointer) error {
	if _, err := p.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	return binary.Write(p.rws, binary.LittleEndian, mp)
}

func (p *MetaPager) SlotMetadata(offset uint64) ([]byte, error) {
	if _, err := p.rws.Seek(int64(offset)+(8*N+16), io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, p.rws.PageSize()-(8*N+16))
	if _, err := p.rws.Read(buf); err != nil {
		return nil, err
	}
	// the first four bytes represents the length
	length := binary.LittleEndian.Uint32(buf[:4])
	return buf[4 : 4+length], nil
}

func (p *MetaPager) SetSlotMetadata(offset uint64, data []byte) error {
	if len(data) > p.rws.SlotSize()-(8*N+16) {
		return errors.New("metadata too large")
	}
	if _, err := p.rws.Seek(int64(offset)+(8*N+16), io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := p.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (p *MetaPager) SlotNextNOffsets(offset uint64, offsets []uint64) ([]uint64, error) {
	if _, err := p.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}

	for i := 0; i < N; i++ {
		if err := binary.Read(p.rws, binary.LittleEndian, &offsets[i]); err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

func (p *MetaPager) SetSlotNextNOffsets(offset uint64, offsets []uint64) error {
	if len(offsets) > N {
		return fmt.Errorf("too many offsets, max number of offsets should be %d", N)
	}

	if _, err := p.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return err
	}

	for _, offset := range offsets {
		if err := binary.Write(p.rws, binary.LittleEndian, offset); err != nil {
			return err
		}
	}

	if err := binary.Write(p.rws, binary.LittleEndian, ^uint64(0)); err != nil {
		return err
	}
	return nil
}

func (p *MetaPager) NextSlot(offset uint64) (*LinkedMetaSlot, error) {
	if _, err := p.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	var next btree.MemoryPointer
	if err := binary.Read(p.rws, binary.LittleEndian, &next); err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{rws: p.rws, offset: next.Offset}, nil
}

func (p *MetaPager) AddNextSlot(offset uint64) (*LinkedMetaSlot, error) {
	curr, err := p.NextSlot(offset)
	if err != nil {
		return nil, err
	}
	if curr.offset != ^uint64(0) {
		return nil, errors.New("next pointer already exists")
	}

	nextOffset, err := p.rws.NewMeta(nil) // todo here is where we inject the logic
	if err != nil {
		return nil, err
	}
	next := &LinkedMetaSlot{pager: p, rws: p.rws, offset: uint64(nextOffset)}
	if err := next.Reset(); err != nil {
		return nil, err
	}
	// save the next pointer
	if _, err := p.rws.Seek(int64(offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	if err := binary.Write(p.rws, binary.LittleEndian, next.offset); err != nil {
		return nil, err
	}
	return next, nil
}

func (p *MetaPager) SlotExists(offset uint64) (bool, error) {
	if offset == ^uint64(0) {
		return false, nil
	}
	if _, err := p.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return false, err
	}
	if _, err := p.rws.Read(make([]byte, p.rws.PageSize())); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *MetaPager) ResetSlot(offset uint64) error {
	// write a full page of zeros
	emptyPage := make([]byte, p.rws.PageSize())
	binary.LittleEndian.PutUint64(emptyPage[12:20], ^uint64(0))
	if _, err := p.rws.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	if _, err := p.rws.Write(emptyPage); err != nil {
		return err
	}
	return nil
}
