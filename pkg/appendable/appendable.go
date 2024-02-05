package appendable

import (
	"encoding/binary"
	"fmt"
	"strings"
)

/**
 * The structure of an index file is characterized by some pages that point
 * to other pages. Each box below represents a (typically 4kB) page and
 * the arrows indicate that there is a pointer to the next page.
 *
 * +-----------+-----------+    +-------------+    +-------------+    +-------------+
 * |  Page GC  | File Meta | -> | Index Meta  | -> | Index Meta  | -> | Index Meta  |
 * +-----------+-----------+    +-------------+    +-------------+    +-------------+
 *                                     |                  |                  |
 *                                     v                  v                  v
 *                              +~~~~~~~~~~~~~+    +~~~~~~~~~~~~~+    +~~~~~~~~~~~~~+
 *                              |   B+ Tree   |    |   B+ Tree   |    |   B+ Tree   |
 *                              +~~~~~~~~~~~~~+    +~~~~~~~~~~~~~+    +~~~~~~~~~~~~~+
 *
 * Note: By convention, the first FileMeta does not have a pointer to the
 * B+ tree. Instead, the first FileMeta is used to store metadata about the
 * file itself and only contains a next pointer.
 *
 * Additionally, the Page GC page is used by the page file to store free page
 * indexes for garbage collection.
 *
 * Consequentially, the index file cannot be smaller than two pages (typically 8kB).
 */

type Version byte

type Format byte

const (
	FormatJSONL Format = iota
	FormatCSV
)

// FieldType represents the type of data stored in the field, which follows
// JSON types excluding Object and null. Object is broken down into subfields
// and null is not stored.
type FieldType byte

const (
	FieldTypeString FieldType = iota
	FieldTypeInt64
	FieldTypeUint64
	FieldTypeFloat64
	FieldTypeObject
	FieldTypeArray
	FieldTypeBoolean
	FieldTypeNull
)

func (t FieldType) TypescriptType() string {
	components := []string{}
	if t&FieldTypeString != 0 {
		components = append(components, "string")
	}
	if t&FieldTypeInt64 != 0 || t&FieldTypeFloat64 != 0 {
		components = append(components, "number")
	}
	if t&FieldTypeObject != 0 {
		components = append(components, "Record")
	}
	if t&FieldTypeArray != 0 {
		components = append(components, "any[]")
	}
	if t&FieldTypeBoolean != 0 {
		components = append(components, "boolean")
	}
	if t&FieldTypeNull != 0 {
		components = append(components, "null")
	}
	if len(components) == 0 {
		return "unknown"
	}
	return strings.Join(components, " | ")
}

type FileMeta struct {
	Version
	Format
	// An offset to indicate how much data is contained within
	// this index. Note that this is implementation-dependent,
	// so it is not guaranteed to have any uniform interpretation.
	// For example, in JSONL, this is the number of bytes read
	// and indexed so far.
	ReadOffset uint64
}

func (m *FileMeta) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 9)
	buf[0] = byte(m.Version)
	binary.BigEndian.PutUint64(buf[1:], m.ReadOffset)
	return buf, nil
}

func (m *FileMeta) UnmarshalBinary(buf []byte) error {
	if len(buf) < 9 {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.Version = Version(buf[0])
	m.ReadOffset = binary.BigEndian.Uint64(buf[1:])
	return nil
}

type IndexMeta struct {
	FieldName string
	FieldType FieldType
}

func (m *IndexMeta) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+len(m.FieldName)+2)
	binary.BigEndian.PutUint64(buf[0:], uint64(m.FieldType))
	binary.BigEndian.PutUint16(buf[8:], uint16(len(m.FieldName)))
	copy(buf[10:], m.FieldName)
	return buf, nil
}

func (m *IndexMeta) UnmarshalBinary(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.FieldType = FieldType(binary.BigEndian.Uint64(buf[0:]))
	nameLength := binary.BigEndian.Uint16(buf[8:])
	if len(buf) < 10+int(nameLength) {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.FieldName = string(buf[10 : 10+nameLength])
	return nil
}
