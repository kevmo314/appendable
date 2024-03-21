package metapage

import (
	"encoding"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
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
	pager  *MultiPager
	offset uint64
}

func (m *LinkedMetaSlot) Root() (pointer.MemoryPointer, error) {
	return m.pager.Root(m.offset)
}

func (m *LinkedMetaSlot) SetRoot(mp pointer.MemoryPointer) error {
	return m.pager.SetRoot(m.offset, mp)
}

// BPTree returns a B+ tree that uses this meta page as the root
// of the tree. If data is not nil, then it will be used as the
// data source for the tree.
//
// Generally, passing data is required, however if the tree
// consists of only inlined values, it is not necessary.
func (m *LinkedMetaSlot) BPTree(t *btree.BPTree) *btree.BPTree {
	t.MetaPage = m
	t.PageFile = m.pager.rws
	return t
}

func (m *LinkedMetaSlot) Metadata() ([]byte, error) {
	return m.pager.Metadata(m.offset)
}

func (m *LinkedMetaSlot) UnmarshalMetadata(bu encoding.BinaryUnmarshaler) error {
	md, err := m.Metadata()
	if err != nil {
		return err
	}
	return bu.UnmarshalBinary(md)
}

func (m *LinkedMetaSlot) SetMetadata(data []byte) error {
	return m.pager.SetMetadata(m.offset, data)
}

func (m *LinkedMetaSlot) MarshalMetadata(bm encoding.BinaryMarshaler) error {
	buf, err := bm.MarshalBinary()
	if err != nil {
		return err
	}
	return m.SetMetadata(buf)
}

func (m *LinkedMetaSlot) NextNOffsets(offsets []uint64) ([]uint64, error) {
	return m.pager.NextNOffsets(m.offset, offsets)
}

func (m *LinkedMetaSlot) SetNextNOffsets(offsets []uint64) error {
	return m.pager.SetNextNOffsets(m.offset, offsets)
}

func (m *LinkedMetaSlot) Next() (*LinkedMetaSlot, error) {
	return m.pager.Next(m.offset)
}

func (m *LinkedMetaSlot) AddNext() (*LinkedMetaSlot, error) {
	return m.pager.AddNext(m.offset)
}

func (m *LinkedMetaSlot) MemoryPointer() pointer.MemoryPointer {
	return pointer.MemoryPointer{Offset: m.offset, Length: 24}
}

func (m *LinkedMetaSlot) Exists() (bool, error) {
	return m.pager.Exists(m.offset)
}

func (m *LinkedMetaSlot) Reset() error {
	return m.pager.Reset(m.offset)
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

func NewMultiBPTree(t pagefile.ReadWriteSeekPager, ms *MultiPager, page int) (*LinkedMetaSlot, error) {
	offset, err := t.Page(0)
	if err != nil {
		return nil, err
	}
	return &LinkedMetaSlot{pager: ms, offset: uint64(offset)}, nil
}
