package appendable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/protocol"
)

type DataHandler interface {
	io.ReadSeeker
	Synchronize(f *IndexFile) error
}

func NewIndexFile(data DataHandler, logger *slog.Logger) (*IndexFile, error) {
	f := &IndexFile{
		Version: CurrentVersion,
		Indexes: []Index{},
		data:    data,
		Logger:  logger,
	}
	return f, data.Synchronize(f)
}

func ReadIndexFile(r io.Reader, data DataHandler, logger *slog.Logger) (*IndexFile, error) {
	f := &IndexFile{
		Logger: logger,
	}

	f.data = data

	// read the version
	version, err := encoding.ReadByte(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	f.Version = protocol.Version(version)

	switch version {
	case 1:
		// read the index file header
		ifh := protocol.IndexFileHeader{}
		if ifh.IndexLength, err = encoding.ReadUint64(r); err != nil {
			return nil, fmt.Errorf("failed to read index file header: %w", err)
		}
		if ifh.DataCount, err = encoding.ReadUint64(r); err != nil {
			return nil, fmt.Errorf("failed to read index file header: %w", err)
		}

		// read the index headers
		f.Indexes = []Index{}
		br := 0
		recordCounts := []uint64{}
		for br < int(ifh.IndexLength) {
			var index Index
			if index.FieldName, err = encoding.ReadString(r); err != nil {
				return nil, fmt.Errorf("failed to read index header: %w", err)
			}
			ft, err := encoding.ReadUint64(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read index header: %w", err)
			}
			index.FieldType = protocol.FieldType(ft)
			recordCount, err := encoding.ReadUint64(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read index header: %w", err)
			}
			recordCounts = append(recordCounts, recordCount)
			index.IndexRecords = make(map[any][]protocol.IndexRecord)
			f.Indexes = append(f.Indexes, index)
			br += encoding.SizeString(index.FieldName) + binary.Size(ft) + binary.Size(uint64(0))
		}
		if br != int(ifh.IndexLength) {
			return nil, fmt.Errorf("expected to read %d bytes, read %d bytes", ifh.IndexLength, br)
		}

		// read the index records
		for i, index := range f.Indexes {

			for j := 0; j < int(recordCounts[i]); j++ {
				var ir protocol.IndexRecord
				if ir.DataNumber, err = encoding.ReadUint64(r); err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}
				if ir.FieldStartByteOffset, err = encoding.ReadUint64(r); err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}
				if ir.FieldLength, err = encoding.UnpackFint16(r); err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}

				var value any
				switch handler := data.(type) {
				case JSONLHandler:
					value, err = ir.Token(handler)
				case CSVHandler:
					value, err = ir.CSVField(handler)
				default:
					err = fmt.Errorf("unrecognized data handler type: %T", handler)
				}

				if err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}

				switch value.(type) {
				case nil, bool, int, int8, int16, int32, int64, float32, float64, string:
					fmt.Printf("appending: %v", value)
					index.IndexRecords[value] = append(index.IndexRecords[value], ir)
				default:
					return nil, fmt.Errorf("unsupported type: %T", value)
				}
			}
		}

		// read the data ranges
		f.EndByteOffsets = make([]uint64, ifh.DataCount)
		for i := 0; i < int(ifh.DataCount); i++ {
			if f.EndByteOffsets[i], err = encoding.ReadUint64(r); err != nil {
				return nil, fmt.Errorf("failed to read data range: %w", err)
			}
		}

		// read the checksums
		f.Checksums = make([]uint64, ifh.DataCount)
		for i := 0; i < int(ifh.DataCount); i++ {
			if f.Checksums[i], err = encoding.ReadUint64(r); err != nil {
				return nil, fmt.Errorf("failed to read checksum: %w", err)
			}
		}

		startIndex := 0
		start := uint64(0)
		if _, isCSV := data.(CSVHandler); isCSV && len(f.EndByteOffsets) > 0 {
			start = f.EndByteOffsets[0]
			startIndex = 1
		}

		for i := startIndex; i < int(ifh.DataCount); i++ {

			// this is a hotfix solution. It works great B)
			if _, isCsv := data.(CSVHandler); isCsv {
				if i > 1 {
					start -= 1
				}
			}

			// read the range from the data file to verify the checksum
			if _, err := data.Seek(int64(start), io.SeekStart); err != nil {
				return nil, fmt.Errorf("failed to seek data file: %w", err)
			}
			buf := &bytes.Buffer{}

			if _, err := io.CopyN(buf, data, int64(f.EndByteOffsets[i]-start-1)); err != nil {
				return nil, fmt.Errorf("failed to read data file: %w", err)
			}

			if xxhash.Sum64(buf.Bytes()) != f.Checksums[i] {
				return nil, fmt.Errorf("checksum mismatch a %d, b %d", xxhash.Sum64(buf.Bytes()), f.Checksums[i])
			}
			start = f.EndByteOffsets[i] + 1
		}
	default:
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// we've deserialized the underlying file, seek to the end of the last data range to prepare for appending
	if len(f.EndByteOffsets) > 0 {
		if _, err := data.Seek(int64(f.EndByteOffsets[len(f.EndByteOffsets)-1]), io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek data file: %w", err)
		}
	}

	return f, data.Synchronize(f)
}

func (f *IndexFile) Serialize(w io.Writer) error {
	// write the version
	if err := encoding.WriteByte(w, byte(f.Version)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	dataCount := uint64(len(f.EndByteOffsets))
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
		if err := encoding.WriteUint64(w, uint64(index.FieldType)); err != nil {
			return fmt.Errorf("failed to write index field type: %w", err)
		}
		// total the number of index records
		count := 0
		for _, records := range index.IndexRecords {
			count += len(records)
		}
		if err := encoding.WriteUint64(w, uint64(count)); err != nil {
			return fmt.Errorf("failed to write index record count: %w", err)
		}
	}

	// write the index records
	for _, index := range f.Indexes {
		var err error
		keys := make([]any, len(index.IndexRecords))
		i := 0
		for key := range index.IndexRecords {
			keys[i] = key
			i++
		}

		sort.Slice(keys, func(i, j int) bool {
			at, bt := keys[i], keys[j]

			switch f.data.(type) {
			case CSVHandler:

				if atr, btr := fieldRankCsvField(at), fieldRankCsvField(bt); atr != btr {
					return atr < btr
				}

				switch at.(type) {
				case nil:
					return false
				case bool:
					return !at.(bool) && bt.(bool)
				case int, int8, int16, int32, int64:
					return at.(int) < bt.(int)
				case float32, float64:
					return at.(float64) < bt.(float64)
				case string:
					return strings.Compare(at.(string), bt.(string)) < 0
				default:
					panic("unknown type")
				}

			case JSONLHandler:
				if atr, btr := fieldRank(at), fieldRank(bt); atr != btr {
					return atr < btr
				}
				switch at.(type) {
				case nil:
					return false
				case bool:
					return !at.(bool) && bt.(bool)
				case int, int8, int16, int32, int64, float32, float64:
					return at.(float64) < bt.(float64)
				case string:
					return strings.Compare(at.(string), bt.(string)) < 0
				default:
					panic("unknown type")
				}
			default:
				panic("unknown handler")
			}
		})
		// iterate in key-ascending order
		for _, key := range keys {
			for _, item := range index.IndexRecords[key] {
				if err = encoding.WriteUint64(w, item.DataNumber); err != nil {
					return fmt.Errorf("failed to write index record: %w", err)
				}
				if err = encoding.WriteUint64(w, item.FieldStartByteOffset); err != nil {
					return fmt.Errorf("failed to write index record: %w", err)
				}
				if err = encoding.PackFint16(w, item.FieldLength); err != nil {
					return fmt.Errorf("failed to write index record: %w", err)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("failed to write index record: %w", err)
		}
	}

	// write the data ranges
	for _, offset := range f.EndByteOffsets {
		if err := encoding.WriteUint64(w, offset); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
	}
	for _, checksum := range f.Checksums {
		if err := encoding.WriteUint64(w, checksum); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
	}

	return nil
}
