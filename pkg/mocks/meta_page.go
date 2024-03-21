package main

import (
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"io"
)

type testMetaPage struct {
	pf   *pagefile.PageFile
	root btree.MemoryPointer
}

func (m *testMetaPage) SetRoot(mp btree.MemoryPointer) error {
	m.root = mp
	return m.write()
}

func (m *testMetaPage) Root() (btree.MemoryPointer, error) {
	return m.root, nil
}

func (m *testMetaPage) write() error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, m.root.Offset)
	if _, err := m.pf.Seek(4096, io.SeekStart); err != nil {
		return err
	}
	if _, err := m.pf.Write(buf); err != nil {
		return err
	}
	return nil
}

func newTestMetaPage(pf *pagefile.PageFile) (*testMetaPage, error) {
	meta := &testMetaPage{pf: pf}
	offset, err := pf.NewPage([]byte{0, 0, 0, 0, 0, 0, 0, 0}, nil)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	// first page is garbage collection
	if offset != 4096 {
		return nil, fmt.Errorf("expected offset 0, got %d", offset)
	}
	return meta, nil
}
