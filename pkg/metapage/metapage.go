package metapage

import (
	"github.com/kevmo314/appendable/pkg/pointer"
	"io"
)

// MetaPage is an abstract interface over the root page of a bptree
// This allows the caller to control the memory location of the meta
// pointer
type MetaPage interface {
	Root() (pointer.MemoryPointer, error)
	SetRoot(pointer.MemoryPointer) error
}

type NodeSerializable interface {
	Size() int64
	NumPointers() int
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
	WriteTo(w io.Writer) (int64, error)
}