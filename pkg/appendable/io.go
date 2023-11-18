package appendable

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/btree"
	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/protocol"
)

func ReadIndexFile(r io.Reader, f *IndexFile) error {
	// read the version
	version, err := encoding.ReadByte(r)
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	f.Version = protocol.Version(version)

	switch version {
	case 1:
		// read the index file header
		ifh := protocol.IndexFileHeader{}
		if ifh.IndexLength, err = encoding.ReadUint64(r); err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}
		if ifh.DataCount, err = encoding.ReadUint64(r); err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}

		// read the index headers
		f.Indexes = []Index{}
		br := 0
		recordCounts := []uint64{}
		for br < int(ifh.IndexLength) {
			var i Index
			if i.FieldName, err = encoding.ReadString(r); err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			ft, err := encoding.ReadByte(r)
			if err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			i.FieldType = protocol.FieldType(ft)
			recordCount, err := encoding.ReadUint64(r)
			if err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			recordCounts = append(recordCounts, recordCount)
			i.IndexRecords = btree.NewG[protocol.IndexRecord](2, f.less)
			f.Indexes = append(f.Indexes, i)
			br += encoding.SizeString(i.FieldName) + binary.Size(ft) + binary.Size(uint64(0))
		}
		if br != int(ifh.IndexLength) {
			return fmt.Errorf("expected to read %d bytes, read %d bytes", ifh.IndexLength, br)
		}

		// read the index records
		for i, index := range f.Indexes {
			for j := 0; j < int(recordCounts[i]); j++ {
				var ir protocol.IndexRecord
				if ir.DataIndex, err = encoding.ReadUint64(r); err != nil {
					return fmt.Errorf("failed to read index record: %w", err)
				}
				if ir.FieldStartByteOffset, err = encoding.ReadUint32(r); err != nil {
					return fmt.Errorf("failed to read index record: %w", err)
				}
				if ir.FieldEndByteOffset, err = encoding.ReadUint32(r); err != nil {
					return fmt.Errorf("failed to read index record: %w", err)
				}
				if _, removed := index.IndexRecords.ReplaceOrInsert(ir); removed {
					return fmt.Errorf("duplicate index record found")
				}
			}
		}

		// read the data ranges
		f.DataRanges = make([]protocol.DataRange, ifh.DataCount)
		for i := 0; i < int(ifh.DataCount); i++ {
			var dr protocol.DataRange
			if dr.StartByteOffset, err = encoding.ReadUint64(r); err != nil {
				return fmt.Errorf("failed to read data range: %w", err)
			}
			if dr.EndByteOffset, err = encoding.ReadUint64(r); err != nil {
				return fmt.Errorf("failed to read data range: %w", err)
			}
			if dr.Checksum, err = encoding.ReadUint64(r); err != nil {
				return fmt.Errorf("failed to read data range: %w", err)
			}
			f.DataRanges[i] = dr
		}
	default:
		return fmt.Errorf("unsupported version: %d", version)
	}
	return nil
}

func WriteIndexFile(w io.Writer, f *IndexFile) error {
	// write the version
	if err := encoding.WriteByte(w, byte(f.Version)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	dataCount := uint64(len(f.DataRanges))
	indexLength := 0
	for _, index := range f.Indexes {
		indexLength += encoding.SizeString(index.FieldName)
		indexLength += binary.Size(index.FieldType)
		indexLength += binary.Size(uint64(0))
	}

	// write the index file header
	if err := encoding.WriteUint64(w, uint64(indexLength)); err != nil {
		return fmt.Errorf("failed to write index length: %w", err)
	}
	if err := encoding.WriteUint64(w, dataCount); err != nil {
		return fmt.Errorf("failed to write data count: %w", err)
	}

	// write the index headers
	for _, index := range f.Indexes {
		if err := encoding.WriteString(w, index.FieldName); err != nil {
			return fmt.Errorf("failed to write index field name: %w", err)
		}
		if err := encoding.WriteByte(w, byte(index.FieldType)); err != nil {
			return fmt.Errorf("failed to write index field type: %w", err)
		}
		if err := encoding.WriteUint64(w, uint64(index.IndexRecords.Len())); err != nil {
			return fmt.Errorf("failed to write index record count: %w", err)
		}
	}

	// write the index records
	for _, index := range f.Indexes {
		var err error
		index.IndexRecords.Ascend(func(item protocol.IndexRecord) bool {
			if err = encoding.WriteUint64(w, item.DataIndex); err != nil {
				return false
			}
			if err = encoding.WriteUint32(w, item.FieldStartByteOffset); err != nil {
				return false
			}
			if err = encoding.WriteUint32(w, item.FieldEndByteOffset); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("failed to write index record: %w", err)
		}
	}

	// write the data ranges
	for _, dataRange := range f.DataRanges {
		if err := encoding.WriteUint64(w, dataRange.StartByteOffset); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
		if err := encoding.WriteUint64(w, dataRange.EndByteOffset); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
		if err := encoding.WriteUint64(w, dataRange.Checksum); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
	}

	return nil
}
