package metapage

import (
	"encoding"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

const N = 16

/**
 * LinkedMetaSlot resides in a Page where each Page contains at most 16 LinkedMetaSlots.
 * Each meta page contains a pointer to the root of the B+ tree, a pointer to the next N meta pages,
 * and the remainder of the page is allocated as free space for metadata.
 *
 * A page exists if and only if the offset is not math.MaxUint64 and the
 * read/write/seek pager can read one full page at the offset. The last
 * page in the linked list will have a next pointer with offset
 * math.MaxUint64.
 */
type LinkedMetaSlot struct {
	rws    pagefile.ReadWriteSeekPager
	pager  *MetaPager
	offset uint64
}

func (m *LinkedMetaSlot) Root() (btree.MemoryPointer, error) {
	return m.pager.SlotRoot(m.offset)
}

func (m *LinkedMetaSlot) SetRoot(mp btree.MemoryPointer) error {
	return m.pager.SetSlotRoot(m.offset, mp)
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
	// t.Pager = m.pager
	return t
}

func (m *LinkedMetaSlot) Metadata() ([]byte, error) {
	return m.pager.SlotMetadata(m.offset)
}

func (m *LinkedMetaSlot) UnmarshalMetadata(bu encoding.BinaryUnmarshaler) error {
	md, err := m.Metadata()
	if err != nil {
		return err
	}
	return bu.UnmarshalBinary(md)
}

func (m *LinkedMetaSlot) SetMetadata(data []byte) error {
	return m.pager.SetSlotMetadata(m.offset, data)
}

func (m *LinkedMetaSlot) MarshalMetadata(bm encoding.BinaryMarshaler) error {
	buf, err := bm.MarshalBinary()
	if err != nil {
		return err
	}
	return m.SetMetadata(buf)
}

func (m *LinkedMetaSlot) NextNOffsets(offsets []uint64) ([]uint64, error) {
	return m.pager.SlotNextNOffsets(m.offset, offsets)
}

func (m *LinkedMetaSlot) SetNextNOffsets(offsets []uint64) error {
	return m.pager.SetSlotNextNOffsets(m.offset, offsets)
}

func (m *LinkedMetaSlot) Next() (*LinkedMetaSlot, error) {
	return m.pager.NextSlot(m.offset)
}

func (m *LinkedMetaSlot) AddNext() (*LinkedMetaSlot, error) {
	return m.pager.AddNextSlot(m.offset)
}

func (m *LinkedMetaSlot) MemoryPointer() btree.MemoryPointer {
	return btree.MemoryPointer{Offset: m.offset, Length: 24}
}

func (m *LinkedMetaSlot) Exists() (bool, error) {
	return m.pager.SlotExists(m.offset)
}

func (m *LinkedMetaSlot) Reset() error {
	return m.pager.ResetSlot(m.offset)
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
