package metapage

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"io"

	"github.com/kevmo314/appendable/pkg/pagefile"
)

const N = 16

/**
 * LinkedMetaSlot is a linked list of meta pages. Each page contains
 * a pointer to the root of the B+ tree, a pointer to the next meta page,
 * and the remainder of the page is allocated as free space for metadata.
 *
 * A page exists if and only if the offset is not math.MaxUint64 and the
 * read/write/seek pager can read one full page at the offset. The last
 * page in the linked list will have a next pointer with offset
 * math.MaxUint64.
 */
type LinkedMetaSlot struct {
	rws    pagefile.ReadWriteSeekPager
	offset uint64
}

func (m *LinkedMetaSlot) Root() (btree.MemoryPointer, error) {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return btree.MemoryPointer{}, err
	}
	var mp btree.MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *LinkedMetaSlot) SetRoot(mp btree.MemoryPointer) error {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return err
	}
	return binary.Write(m.rws, binary.LittleEndian, mp)
}

// BPTree returns a B+ tree that uses this meta page as the root
// of the tree. If data is not nil, then it will be used as the
// data source for the tree.
//
// Generally, passing data is required, however if the tree
// consists of only inlined values, it is not necessary.
func (m *LinkedMetaSlot) BPTree(t *btree.BPTree) *btree.BPTree {
	t.PageFile = m.rws
	t.MetaPage = m
	return t
}

func (m *LinkedMetaSlot) Metadata() ([]byte, error) {
	if _, err := m.rws.Seek(int64(m.offset)+(8*N+16), io.SeekStart); err != nil {
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

func (m *LinkedMetaSlot) UnmarshalMetadata(bu encoding.BinaryUnmarshaler) error {
	md, err := m.Metadata()
	if err != nil {
		return err
	}
	return bu.UnmarshalBinary(md)
}

func (m *LinkedMetaSlot) SetMetadata(data []byte) error {
	if len(data) > m.rws.PageSize()-(8*N+16) {
		return errors.New("metadata too large")
	}
	if _, err := m.rws.Seek(int64(m.offset)+(8*N+16), io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := m.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (m *LinkedMetaSlot) MarshalMetadata(bm encoding.BinaryMarshaler) error {
	buf, err := bm.MarshalBinary()
	if err != nil {
		return err
	}
	return m.SetMetadata(buf)
}

func (m *LinkedMetaSlot) NextNOffsets(offsets []uint64) ([]uint64, error) {
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return nil, err
	}

	for i := 0; i < N; i++ {
		if err := binary.Read(m.rws, binary.LittleEndian, &offsets[i]); err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

func (m *LinkedMetaSlot) SetNextNOffsets(offsets []uint64) error {
	if len(offsets) > N {
		return fmt.Errorf("too many offsets, max number of offsets should be %d", N)
	}

	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
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

func (m *LinkedMetaSlot) Next() (*LinkedMetaSlot, error) {
	if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
		return nil, err
	}
	var next btree.MemoryPointer
	if err := binary.Read(m.rws, binary.LittleEndian, &next); err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{rws: m.rws, offset: next.Offset}, nil
}

func (m *LinkedMetaSlot) AddNext() (*LinkedMetaSlot, error) {
	curr, err := m.Next()
	if err != nil {
		return nil, err
	}
	if curr.offset != ^uint64(0) {
		return nil, errors.New("next pointer already exists")
	}
	offset, err := m.rws.NewPage(nil)
	if err != nil {
		return nil, err
	}
	next := &LinkedMetaSlot{rws: m.rws, offset: uint64(offset)}
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

func (m *LinkedMetaSlot) MemoryPointer() btree.MemoryPointer {
	return btree.MemoryPointer{Offset: m.offset, Length: 24}
}

func (m *LinkedMetaSlot) Exists() (bool, error) {
	if m.offset == ^uint64(0) {
		return false, nil
	}
	// attempt to read the page
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
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

func (m *LinkedMetaSlot) Reset() error {
	// write a full page of zeros
	emptyPage := make([]byte, m.rws.PageSize())
	binary.LittleEndian.PutUint64(emptyPage[12:20], ^uint64(0))
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return err
	}
	if _, err := m.rws.Write(emptyPage); err != nil {
		return err
	}
	return nil
}

// Collect returns a slice of all linked meta pages from this page to the end.
// This function is useful for debugging and testing, however generally it should
// not be used for functional code.
func (m *LinkedMetaSlot) Collect() ([]*LinkedMetaSlot, error) {
	var pages []*LinkedMetaSlot
	node := m
	for {
		exists, err := node.Exists()
		if err != nil {
			return nil, err
		}
		if !exists {
			break
		}
		pages = append(pages, node)
		next, err := node.Next()
		if err != nil {
			return nil, err
		}
		node = next
	}
	return pages, nil
}

func (m *LinkedMetaSlot) String() string {
	nm, err := m.Next()
	if err != nil {
		panic(err)
	}
	root, err := m.Root()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("LinkedMetaSlot{offset: %x,\tnext: %x,\troot: %x}", m.offset, nm.offset, root.Offset)
}

func NewMultiBPTree(t pagefile.ReadWriteSeekPager, page int) (*LinkedMetaSlot, error) {
	offset, err := t.Page(0)
	if err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{rws: t, offset: uint64(offset)}, nil
}
