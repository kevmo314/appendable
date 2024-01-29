package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

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

func (m *LinkedMetaPage) BPTree() *BPTree {
	return NewBPTree(m.rws, m)
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
	return &LinkedMetaPage{rws: m.rws, offset: next.Offset}, nil
}

func (m *LinkedMetaPage) AddNext() (*LinkedMetaPage, error) {
	curr, err := m.Next()
	if err != nil {
		return nil, err
	}
	if curr.offset != ^uint64(0) {
		return nil, errors.New("next pointer already exists")
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

func (m *LinkedMetaPage) Reset() error {
	// write a full page of zeros
	emptyPage := make([]byte, m.rws.PageSize())
	binary.BigEndian.PutUint64(emptyPage[12:20], ^uint64(0))
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

func (m *LinkedMetaPage) String() string {
	nm, err := m.Next()
	if err != nil {
		panic(err)
	}
	root, err := m.Root()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("LinkedMetaPage{offset: %x,\tnext: %x,\troot: %x}", m.offset, nm.offset, root.Offset)
}

func NewMultiBPTree(t ReadWriteSeekPager, page int) (*LinkedMetaPage, error) {
	offset, err := t.Page(0)
	if err != nil {
		return nil, err
	}
	return &LinkedMetaPage{rws: t, offset: uint64(offset)}, nil
}
