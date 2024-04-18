package metapage

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pointer"

	"github.com/kevmo314/appendable/pkg/pagefile"
)

var errNotAPage = errors.New("this is not a page, did you forget to call .Next() on a tree?")

/**
 * LinkedMetaPage is a linked list of meta pages. Each page contains
 * a pointer to the root of the B+ tree, a pointer to the next meta page,
 * and the remainder of the page is allocated as free space for metadata.
 *
 * A page exists if and only if the offset is not math.MaxUint64 and the
 * read/write/seek pager can read one full page at the offset. The last
 * page in the linked list will have a next pointer with offset
 * math.MaxUint64.
 */
type LinkedMetaPage struct {
	rws pagefile.ReadWriteSeekPager

	// offset is the byte offset of the page in the page file.
	offset uint64

	index uint8
}

/**

Each page is structured:
+-------------------------+--------------+
| 12 bytes - Next pointer | 1 byte count | // this count is special. See below
+-------------------------+--------------+
... <count> slots ...


+-------------------------+------------------+-----------------------+
| 12 bytes - root pointer | 1 byte of length | 256 bytes of metadata | // we consider this one linked meta slot.
+-------------------------+------------------+-----------------------+
       ...
+-------------------------+------------------+-----------------------+
| 12 bytes - root pointer | 1 byte of length | 256 bytes of metadata |
+-------------------------+------------------+-----------------------+

Since width = root pointer + 1 byte of length + metadata,
The SLOT_WIDTH is 12 + 1 + 256 for a given slot.

12 + 1 + (index) * (12 + 1 + 256) = 4048 bytes for count 15

0th index slot => 12 + 1
ith index slot => 12 + 1 + <width of the ith slot> => 12 + 1 + i * SLOT_WIDTH
i+1th index slot = > 12 + 1 + <width of the i+1th slot> => 12 + 1 + (i + 1) + SLOT_WIDTH);
NewBTree( page num ) => LinkedMetaPage
*/

var pointerBytes = uint64(12)
var countByte = uint64(1)

func (m *LinkedMetaPage) Root() (pointer.MemoryPointer, error) {
	if m.index == ^uint8(0) {
		return pointer.MemoryPointer{}, errNotAPage
	}
	if _, err := m.rws.Seek(int64(m.rootMemoryPointerPageOffset()), io.SeekStart); err != nil {
		return pointer.MemoryPointer{}, err
	}
	var mp pointer.MemoryPointer
	return mp, binary.Read(m.rws, binary.LittleEndian, &mp)
}

func (m *LinkedMetaPage) SetRoot(mp pointer.MemoryPointer) error {
	if m.index == ^uint8(0) {
		return errNotAPage
	}
	if _, err := m.rws.Seek(int64(m.rootMemoryPointerPageOffset()), io.SeekStart); err != nil {
		return err
	}
	return binary.Write(m.rws, binary.LittleEndian, mp)
}

// btree.BTree returns a B+ tree that uses this meta page as the root
// of the tree. If data is not nil, then it will be used as the
// data source for the tree.
//
// Generally, passing data is required, however if the tree
// consists of only inlined values, it is not necessary.
func (m *LinkedMetaPage) BTree(t *btree.BTree) *btree.BTree {
	t.PageFile = m.rws
	t.MetaPage = m
	return t
}

func (m *LinkedMetaPage) Metadata() ([]byte, error) {
	if m.index == ^uint8(0) {
		return nil, errNotAPage
	}
	if _, err := m.rws.Seek(int64(m.rootMemoryPointerPageOffset())+24, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, 4+m.rws.SlotSize())
	if _, err := m.rws.Read(buf); err != nil {
		return nil, err
	}
	// the first four bytes represents the length
	length := binary.LittleEndian.Uint32(buf[:4])
	return buf[4 : 4+length], nil
}

func (m *LinkedMetaPage) UnmarshalMetadata(bu encoding.BinaryUnmarshaler) error {
	md, err := m.Metadata()
	if err != nil {
		return err
	}
	return bu.UnmarshalBinary(md)
}

func (m *LinkedMetaPage) SetMetadata(data []byte) error {
	if m.index == ^uint8(0) {
		return errNotAPage
	}
	if len(data) > m.rws.SlotSize() {
		return errors.New("metadata too large")
	}
	if _, err := m.rws.Seek(int64(m.rootMemoryPointerPageOffset())+24, io.SeekStart); err != nil {
		return err
	}
	buf := append(make([]byte, 4), data...)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := m.rws.Write(buf); err != nil {
		return err
	}
	return nil
}

func (m *LinkedMetaPage) MarshalMetadata(bm encoding.BinaryMarshaler) error {
	buf, err := bm.MarshalBinary()
	if err != nil {
		return err
	}
	return m.SetMetadata(buf)
}

func (m *LinkedMetaPage) nextPageOffset() (uint64, error) {
	if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
		return 0, err
	}
	var next uint64
	return next, binary.Read(m.rws, binary.LittleEndian, &next)
}

func (m *LinkedMetaPage) count() (uint8, error) {
	if _, err := m.rws.Seek(int64(m.offset)+int64(pointerBytes), io.SeekStart); err != nil {
		return 0, err
	}
	var count uint8
	return count, binary.Read(m.rws, binary.LittleEndian, &count)
}

func (m *LinkedMetaPage) rootMemoryPointerPageOffset() uint64 {
	return m.offset + pointerBytes + countByte + uint64(m.index)*(uint64(m.rws.SlotSize())+pointerBytes+countByte)
}

func (m *LinkedMetaPage) Next() (*LinkedMetaPage, error) {
	// if the current index is less than the count, increment the index.
	count, err := m.count()
	if err != nil {
		return nil, err
	}
	if m.index+1 < count {
		return &LinkedMetaPage{rws: m.rws, offset: m.offset, index: m.index + 1}, nil
	}
	// otherwise, read the next page
	nextOffset, err := m.nextPageOffset()
	if err != nil {
		return nil, err
	}
	if nextOffset == ^uint64(0) {
		// we've reached the end of the linked list
		return nil, io.EOF
	}
	return &LinkedMetaPage{rws: m.rws, offset: uint64(nextOffset)}, nil
}

func (m *LinkedMetaPage) AddNext() (*LinkedMetaPage, error) {
	count, err := m.count()
	if err != nil {
		return nil, err
	}
	if m.index+1 < count {
		return nil, errors.New("next pointer already exists")
	}
	if count != 15 {
		// increment the count
		if _, err := m.rws.Seek(int64(m.offset)+12, io.SeekStart); err != nil {
			return nil, err
		}
		if err := binary.Write(m.rws, binary.LittleEndian, count+1); err != nil {
			return nil, err
		}
		return &LinkedMetaPage{rws: m.rws, offset: m.offset, index: m.index + 1}, nil
	} else {
		// otherwise, read the next page
		nextOffset, err := m.nextPageOffset()
		if err != nil {
			return nil, err
		}
		if nextOffset != ^uint64(0) {
			return nil, errors.New("next pointer already exists")
		}
		offset, err := m.rws.NewPage(nil)
		if err != nil {
			return nil, err
		}
		next := &LinkedMetaPage{rws: m.rws, offset: uint64(offset)}
		if err := next.reset(1); err != nil {
			return nil, err
		}
		// save the next pointer
		if _, err := m.rws.Seek(int64(m.offset), io.SeekStart); err != nil {
			return nil, err
		}
		if err := binary.Write(m.rws, binary.LittleEndian, next.offset); err != nil {
			return nil, err
		}
		return next, nil
	}
}

func (m *LinkedMetaPage) reset(count uint8) error {
	// write a full page of zeros
	emptyPage := make([]byte, m.rws.PageSize())
	binary.LittleEndian.PutUint64(emptyPage[0:8], ^uint64(0))
	emptyPage[8] = count
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
func (m *LinkedMetaPage) Collect() ([]*LinkedMetaPage, error) {
	var pages []*LinkedMetaPage
	node := m
	for {
		if node.index != ^uint8(0) {
			pages = append(pages, node)
		}
		next, err := node.Next()
		if err != nil {
			if err == io.EOF {
				return pages, nil
			}
			return nil, err
		}
		node = next
	}
}

func (m *LinkedMetaPage) String() string {
	nm, err := m.nextPageOffset()
	if err != nil {
		panic(err)
	}
	root, err := m.Root()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("LinkedMetaPage{offset: %x, index: %d,\tnext: %x,\troot: %x}", m.offset, m.index, nm, root.Offset)
}

func NewMultiBTree(t pagefile.ReadWriteSeekPager, page int) (*LinkedMetaPage, error) {
	offset, err := t.Page(0)
	if err != nil {
		return nil, err
	}
	lmp := &LinkedMetaPage{rws: t, offset: uint64(offset), index: ^uint8(0)}
	// attempt to read the page and initialize it if it doesn't already exist
	if _, err := t.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := t.Read(make([]byte, t.PageSize())); err != nil {
		if err == io.EOF {
			// the page doesn't exist, so we need to create it
			if err := lmp.reset(0); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return lmp, nil
}
