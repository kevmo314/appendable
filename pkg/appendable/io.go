package appendable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/kevmo314/appendable/pkg/encoding"
	"github.com/kevmo314/appendable/pkg/protocol"
)

func NewIndexFile(data io.ReadSeeker) (*IndexFile, error) {
	f := &IndexFile{
		Version: CurrentVersion,
		Indexes: []Index{},
		data:    data,
	}
	return f, f.Synchronize()
}

func ReadIndexFile(r io.Reader, data io.ReadSeeker) (*IndexFile, error) {
	f := &IndexFile{}

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
				if ir.FieldStartByteOffset, err = encoding.ReadUint64(r); err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}
				if ir.FieldLength, err = encoding.ReadUint32(r); err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}

				value, err := ir.Token(data)
				if err != nil {
					return nil, fmt.Errorf("failed to read index record: %w", err)
				}

				switch value.(type) {
				case nil, bool, int, int8, int16, int32, int64, float32, float64, string:
					index.IndexRecords[value] = append(index.IndexRecords[value], ir)
				default:
					return nil, fmt.Errorf("unsupported type: %T", value)
				}
			}
		}

		// read the data ranges
		f.DataRanges = make([]protocol.DataRange, ifh.DataCount)
		start := uint64(0)
		for i := 0; i < int(ifh.DataCount); i++ {
			var dr protocol.DataRange
			if dr.EndByteOffset, err = encoding.ReadUint64(r); err != nil {
				return nil, fmt.Errorf("failed to read data range: %w", err)
			}
			if dr.Checksum, err = encoding.ReadUint64(r); err != nil {
				return nil, fmt.Errorf("failed to read data range: %w", err)
			}
			f.DataRanges[i] = dr

			// read the range from the data file to verify the checksum
			if _, err := data.Seek(int64(start), io.SeekStart); err != nil {
				return nil, fmt.Errorf("failed to seek data file: %w", err)
			}
			buf := &bytes.Buffer{}
			if _, err := io.CopyN(buf, data, int64(dr.EndByteOffset-start)); err != nil {
				return nil, fmt.Errorf("failed to read data file: %w", err)
			}

			// verify the checksum
			if xxhash.Sum64(buf.Bytes()) != dr.Checksum {
				return nil, fmt.Errorf("checksum mismatch a %d, b %d", xxhash.Sum64(buf.Bytes()), dr.Checksum)
			}
			start = dr.EndByteOffset + 1
		}
	default:
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// we've deserialized the underlying file, seek to the end of the last data range to prepare for appending
	if len(f.DataRanges) > 0 {
		if _, err := data.Seek(int64(f.DataRanges[len(f.DataRanges)-1].EndByteOffset+1), io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek data file: %w", err)
		}
	}
	return f, f.Synchronize()
}

func (f *IndexFile) Synchronize() error {
	// read until the next newline
	scanner := bufio.NewScanner(f.data)
	for i := 0; scanner.Scan(); i++ {
		log.Printf("%v", i)

		line := scanner.Bytes()

		// create a new json decoder
		dec := json.NewDecoder(bytes.NewReader(line))

		// if the first token is not {, then return an error
		if t, err := dec.Token(); err != nil || t != json.Delim('{') {
			return fmt.Errorf("expected '%U', got '%U' (only json objects are supported at the root)", '{', t)
		}

		if err := f.handleObject(dec, []string{}, uint64(len(f.DataRanges))); err != nil {
			return fmt.Errorf("failed to handle object: %w", err)
		}

		// the next token must be a }
		if t, err := dec.Token(); err != nil || t != json.Delim('}') {
			return fmt.Errorf("expected '}', got '%v'", t)
		}

		// append a data range
		var start uint64
		if len(f.DataRanges) > 0 {
			start = f.DataRanges[len(f.DataRanges)-1].EndByteOffset + 1
		}
		dataRange := protocol.DataRange{
			EndByteOffset: start + uint64(len(line)), // include the newline, so don't subtract 1. recall this is inclusive.
			Checksum:      xxhash.Sum64(line),
		}
		f.DataRanges = append(f.DataRanges, dataRange)
	}
	return nil
}

func fieldRank(token json.Token) int {
	switch token.(type) {
	case nil:
		return 1
	case bool:
		return 2
	case int, int8, int16, int32, int64, float32, float64:
		return 3
	case string:
		return 4
	default:
		panic("unknown type")
	}
}

func (f *IndexFile) Serialize(w io.Writer) error {
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
		})
		// iterate in key-ascending order
		for _, key := range keys {
			for _, item := range index.IndexRecords[key] {
				if err = encoding.WriteUint64(w, item.FieldStartByteOffset); err != nil {
					return fmt.Errorf("failed to write index record: %w", err)
				}
				if err = encoding.WriteUint32(w, item.FieldLength); err != nil {
					return fmt.Errorf("failed to write index record: %w", err)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("failed to write index record: %w", err)
		}
	}

	// write the data ranges
	for _, dataRange := range f.DataRanges {
		if err := encoding.WriteUint64(w, dataRange.EndByteOffset); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
		if err := encoding.WriteUint64(w, dataRange.Checksum); err != nil {
			return fmt.Errorf("failed to write data range: %w", err)
		}
	}

	return nil
}
