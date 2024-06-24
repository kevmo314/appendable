package pointer

import (
	"bytes"
	"fmt"
)

type ReferencedValue struct {
	// it is generally optional to set the DataPointer. if it is not set, the
	// value is taken to be unreferenced and is stored directly in the node.
	// if it is set, the value is used for comparison but the value is stored
	// as a reference to the DataPointer.
	//
	// caveat: DataPointer is used as a disambiguator for the value. the b+ tree
	// implementation does not support duplicate keys and uses the DataPointer
	// to disambiguate between keys that compare as equal.
	DataPointer MemoryPointer
	Value       []byte
}

func (rv ReferencedValue) String() string {
	return fmt.Sprintf("ReferencedValue@%s{%s}", rv.DataPointer, rv.Value)
}

func CompareReferencedValues(a, b ReferencedValue) int {
	if cmp := bytes.Compare(a.Value, b.Value); cmp != 0 {
		return cmp
	} else if a.DataPointer.Offset < b.DataPointer.Offset {
		return -1
	} else if a.DataPointer.Offset > b.DataPointer.Offset {
		return 1
	} else if a.DataPointer.Length < b.DataPointer.Length {
		return -1
	} else if a.DataPointer.Length > b.DataPointer.Length {
		return 1
	}
	return 0
}
