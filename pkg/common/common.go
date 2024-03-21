package common

import "fmt"

// MemoryPointer is a uint64 offset and uint32 length
type MemoryPointer struct {
	Offset uint64
	Length uint32
}

func (mp MemoryPointer) String() string {
	return fmt.Sprintf("Pointer[%08x:%08x]", mp.Offset, mp.Offset+uint64(mp.Length))
}
