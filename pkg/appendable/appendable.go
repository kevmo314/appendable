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
	FieldTypeTrigram
)

func (t FieldType) TypescriptType() string {
	components := []string{}
	if t&FieldTypeString != 0 || t&FieldTypeTrigram != 0 {
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
	buf := make([]byte, 10)
	buf[0] = byte(m.Version)
	buf[1] = byte(m.Format)
	binary.LittleEndian.PutUint64(buf[2:], m.ReadOffset)
	return buf, nil
}

func (m *FileMeta) UnmarshalBinary(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.Version = Version(buf[0])

	fileFormat := buf[1]

	switch fileFormat {
	case byte(0):
		m.Format = FormatJSONL
	case byte(1):
		m.Format = FormatCSV
	default:
		return fmt.Errorf("unrecognized file format: %v", buf[1])
	}

	m.ReadOffset = binary.LittleEndian.Uint64(buf[2:])
	return nil
}

type IndexMeta struct {
	FieldName string
	FieldType FieldType
	Width     uint16
}

func (m *IndexMeta) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 2+len(m.FieldName)+2+2)
	binary.LittleEndian.PutUint16(buf[0:], uint16(m.FieldType))
	binary.LittleEndian.PutUint16(buf[2:], m.Width)
	binary.LittleEndian.PutUint16(buf[4:], uint16(len(m.FieldName)))
	copy(buf[6:], m.FieldName)
	return buf, nil
}

func (m *IndexMeta) UnmarshalBinary(buf []byte) error {
	if len(buf) < 4 {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.FieldType = FieldType(binary.LittleEndian.Uint16(buf[0:]))
	m.Width = binary.LittleEndian.Uint16(buf[2:])
	nameLength := binary.LittleEndian.Uint16(buf[4:])
	if len(buf) < 4+int(nameLength) {
		return fmt.Errorf("invalid metadata size: %d", len(buf))
	}
	m.FieldName = string(buf[6 : 6+nameLength])
	return nil
}

func DetermineType(ft FieldType) uint16 {
	shift := 1 // we'll dedicate 0 to be variable width, everything else is the fixed width + shift
	width := uint16(0)
	switch ft {
	case FieldTypeBoolean:
		width = uint16(shift + 1)
	case FieldTypeNull:
		width = uint16(shift + 0)
	case FieldTypeFloat64, FieldTypeInt64, FieldTypeUint64:
		width = uint16(shift + 8)
	}

	return width
}
