package appendable

import (
	"errors"
	"fmt"
	"github.com/kevmo314/appendable/pkg/linkedpage"
	"io"
	"time"

	"github.com/kevmo314/appendable/pkg/bptree"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

const CurrentVersion = 1

type DataHandler interface {
	bptree.DataParser
	Synchronize(f *IndexFile, df []byte) error
	Format() Format
}

// IndexFile is a representation of the entire index file.
type IndexFile struct {
	tree        *linkedpage.LinkedMetaPage
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

	tree, err := linkedpage.NewMultiBPTree(pf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi b+ tree: %w", err)
	}
	// ensure the first page is written.
	node, err := tree.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to get next meta page: %w", err)
	}
	if errors.Is(err, io.EOF) {
		// the page doesn't exist, so we need to create it
		created, err := tree.AddNext()
		if err != nil {
			return nil, fmt.Errorf("failed to add next meta page: %w", err)
		}
		metadata := &FileMeta{
			Version: CurrentVersion,
			Format:  dataHandler.Format(),
		}
		buf, err := metadata.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata: %w", err)
		}
		if err := created.SetMetadata(buf); err != nil {
			return nil, fmt.Errorf("failed to set metadata: %w", err)
		}
		return &IndexFile{tree: created, dataHandler: dataHandler, pf: pf, searchHeaders: searchHeaders}, nil
	} else {
		// validate the metadata
		buf, err := node.Metadata()
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
		return &IndexFile{tree: node, dataHandler: dataHandler, pf: pf, searchHeaders: searchHeaders}, nil
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

func (i *IndexFile) Indexes() (*linkedpage.LinkedMetaPage, error) {
	return i.tree.Next()
}

func (i *IndexFile) IsEmpty() (bool, error) {
	n, err := i.tree.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("failed to get next meta page: %w", err)
	}
	return n != nil, nil
}

func (i *IndexFile) IndexFieldNames() ([]string, error) {
	var fieldNames []string
	uniqueFieldNames := make(map[string]bool)

	mp := i.tree

	for {
		next, err := mp.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to get next meta page: %w", err)
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

func (i *IndexFile) FindOrCreateIndex(name string, fieldType FieldType) (*linkedpage.LinkedMetaPage, *IndexMeta, error) {
	mp := i.tree
	for {
		next, err := mp.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, fmt.Errorf("failed to get next meta page: %w", err)
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
	metadata.TotalFieldValueLength = uint64(0)
	buf, err := metadata.MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return next, metadata, next.SetMetadata(buf)
}

// Synchronize will synchronize the index file with the data file.
// This is a convenience method and is equivalent to calling
// Synchronize() on the data handler itself.
func (i *IndexFile) Synchronize(df []byte) error {
	return i.dataHandler.Synchronize(i, df)
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
