package btree

import (
	"github.com/kevmo314/appendable/pkg/metapage"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
)

type BTree struct {
	MetaPage metapage.MetaPage
	PageFile pagefile.ReadWriteSeekPager

	Width uint16
}

func (t *BTree) root() (*BTreeNode, pointer.MemoryPointer, error) {
	mp, err := t.MetaPage.Root()
	if err != nil {
		return nil, mp, err
	}

	root, err := t.readNode(mp.Offset)
	if err != nil {
		return nil, mp, err
	}

	return root, mp, nil
}

func (t *BTree) readNode(offset uint64) (*BTreeNode, error) {
	if _, err := t.PageFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	node := &BTreeNode{Width: t.Width}
	buf := make([]byte, t.PageFile.PageSize())

	if _, err := t.PageFile.Read(buf); err != nil {
		return nil, err
	}

	if err := node.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	return node, nil
}
