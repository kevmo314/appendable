package appendable

import (
	"fmt"
	"github.com/kevmo314/appendable/pkg/metapage"
	"io"
	"time"

	"github.com/kevmo314/appendable/pkg/btree"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

const CurrentVersion = 1

type DataHandler interface {
	btree.DataParser
	Synchronize(f *IndexFile, df []byte) error
	Format() Format
}

// IndexFile is a representation of the entire index file.
type IndexFile struct {
	tree        *metapage.LinkedMetaPage
	dataHandler DataHandler

	pf                *pagefile.PageFile
	BenchmarkCallback func(int)

	searchHeaders []string
}

func NewIndexFile(f io.ReadWriteSeeker, dataHandler DataHandler, searchHeaders []string) (*IndexFile, error) {
	pf, err := pagefile.NewPageFile(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create page file: %w", err)
	}

	tree, err := metapage.NewMultiBTree(pf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi b+ tree: %w", err)
	}
	// ensure the first page is written.
	for i := 0; ; i++ {
		exists, err := tree.Exists()
		if err != nil {
			return nil, fmt.Errorf("failed to check if meta page exists: %w", err)
		}
		if !exists {
			if err := tree.Reset(); err != nil {
				return nil, fmt.Errorf("failed to reset meta page: %w", err)
			}
			metadata := &FileMeta{
				Version: CurrentVersion,
				Format:  dataHandler.Format(),
			}
			buf, err := metadata.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal metadata: %w", err)
			}
			if err := tree.SetMetadata(buf); err != nil {
				return nil, fmt.Errorf("failed to set metadata: %w", err)
			}
		} else if i > 1 {
			panic("expected to only reset the first page once")
		} else {
			// validate the metadata
			buf, err := tree.Metadata()
			if err != nil {
				return nil, fmt.Errorf("failed to read metadata: %w", err)
			}
			metadata := &FileMeta{}
			if err := metadata.UnmarshalBinary(buf); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
			if metadata.Version != CurrentVersion {
				return nil, fmt.Errorf("unsupported version: %d", metadata.Version)
			}
			if metadata.Format != dataHandler.Format() {
				return nil, fmt.Errorf("unsupported format: %x", metadata.Format)
			}
			return &IndexFile{tree: tree, dataHandler: dataHandler, pf: pf, searchHeaders: searchHeaders}, nil
		}
	}
}

func (i *IndexFile) Metadata() (*FileMeta, error) {
	// the first page consists of associated metadata for the tree
	buf, err := i.tree.Metadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	metadata := &FileMeta{}
	return metadata, metadata.UnmarshalBinary(buf)
}

func (i *IndexFile) SetMetadata(metadata *FileMeta) error {
	buf, err := metadata.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return i.tree.SetMetadata(buf)
}

func (i *IndexFile) Indexes() (*metapage.LinkedMetaPage, error) {
	return i.tree.Next()
}

func (i *IndexFile) IsEmpty() (bool, error) {
	n, err := i.tree.Next()
	if err != nil {
		return false, fmt.Errorf("failed to get next meta page: %w", err)
	}
	exists, err := n.Exists()
	if err != nil {
		return false, fmt.Errorf("failed to check if meta page exists: %w", err)
	}
	return !exists, nil
}

func (i *IndexFile) IndexFieldNames() ([]string, error) {
	var fieldNames []string
	uniqueFieldNames := make(map[string]bool)

	mp := i.tree

	for {
		next, err := mp.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next meta page: %w", err)
		}
		exists, err := next.Exists()
		if err != nil {
			return nil, fmt.Errorf("failed to check if meta page exists: %w", err)
		}
		if !exists {
			break
		}
		buf, err := next.Metadata()
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}
		metadata := &IndexMeta{}
		if err := metadata.UnmarshalBinary(buf); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		if _, ok := uniqueFieldNames[metadata.FieldName]; !ok {
			uniqueFieldNames[metadata.FieldName] = true
			fieldNames = append(fieldNames, metadata.FieldName)
		}
		mp = next
	}

	return fieldNames, nil
}

func (i *IndexFile) FindOrCreateIndex(name string, fieldType FieldType) (*metapage.LinkedMetaPage, *IndexMeta, error) {
	mp := i.tree
	for {
		// this is done in an odd order to avoid needing to keep track of the previous page
		next, err := mp.Next()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next meta page: %w", err)
		}
		exists, err := next.Exists()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check if meta page exists: %w", err)
		}
		if !exists {
			break
		}
		buf, err := next.Metadata()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read metadata: %w", err)
		}
		metadata := &IndexMeta{}
		if err := metadata.UnmarshalBinary(buf); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
		if metadata.FieldName == name && metadata.FieldType == fieldType {
			return next, metadata, nil
		}
		mp = next
	}
	// we haven't found the index, so we need to create it
	next, err := mp.AddNext()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to add next meta page: %w", err)
	}
	metadata := &IndexMeta{}
	metadata.FieldName = name
	metadata.FieldType = fieldType
	metadata.Width = DetermineType(fieldType)
	metadata.TotalLength = uint64(0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return next, metadata, nil
}

// Synchronize will synchronize the index file with the data file.
// This is a convenience method and is equivalent to calling
// Synchronize() on the data handler itself.
func (i *IndexFile) Synchronize(df []byte) error {
	return i.dataHandler.Synchronize(i, df)
}

func (i *IndexFile) UpdateOffsets() error {
	// first pass to collect all offsets
	mp := i.tree
	var offsets []uint64

	for {
		next, err := mp.Next()
		if err != nil {
			return fmt.Errorf("failed to get next meta page: %w", err)
		}
		exists, err := next.Exists()
		if err != nil {
			return fmt.Errorf("failed to check if meta page exists: %w", err)
		}

		if !exists {
			offsets = append(offsets, ^uint64(0))
			break
		}

		offsets = append(offsets, next.MemoryPointer().Offset)

		mp = next
	}

	mp = i.tree

	for idx := 0; ; idx++ {
		exists, err := mp.Exists()
		if err != nil {
			return fmt.Errorf("failed to check if meta page exists: %w", err)
		}

		if !exists {
			break
		}

		upperBound := idx + metapage.N
		if upperBound > len(offsets) {
			upperBound = len(offsets)
		}

		if err = mp.SetNextNOffsets(offsets[idx:upperBound]); err != nil {
			return fmt.Errorf("failed to set next N offsets: %w", err)
		}

		next, err := mp.Next()
		if err != nil {
			return fmt.Errorf("failed to get next meta page: %w", err)
		}

		mp = next
	}

	return nil
}

func (i *IndexFile) SetBenchmarkFile(f io.Writer) {
	t0 := time.Now()
	i.BenchmarkCallback = func(n int) {
		// write timestamp, number of records, and number of pages
		dt := time.Since(t0)
		fmt.Fprintf(f, "%d,%d,%d\n", dt.Microseconds(), n, i.pf.PageCount())
	}
}

func (i *IndexFile) IsSearch(fieldName string) bool {
	for _, sh := range i.searchHeaders {
		if fieldName == sh {
			return true
		}
	}

	return false
}
