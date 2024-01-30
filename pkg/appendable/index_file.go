package appendable

import (
	"fmt"
	"io"

	"github.com/kevmo314/appendable/pkg/btree"
)

const CurrentVersion = 1

type DataFile interface {
	io.ReadSeeker
	io.ReaderAt
}

type DataHandler interface {
	Synchronize(f *IndexFile, df DataFile) error
	Format() Format
}

// IndexFile is a representation of the entire index file.
type IndexFile struct {
	tree        *btree.LinkedMetaPage
	dataHandler DataHandler
}

func NewIndexFile(f io.ReadWriteSeeker, dataHandler DataHandler) (*IndexFile, error) {
	pf, err := btree.NewPageFile(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create page file: %w", err)
	}
	tree, err := btree.NewMultiBPTree(pf, 0)
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
			return &IndexFile{tree: tree, dataHandler: dataHandler}, nil
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

func (i *IndexFile) Indexes() (*btree.LinkedMetaPage, error) {
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

func (i *IndexFile) FindOrCreateIndex(name string, fieldType FieldType) (*btree.LinkedMetaPage, error) {
	mp := i.tree
	for {
		// this is done in an odd order to avoid needing to keep track of the previous page
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
		if metadata.FieldName == name && metadata.FieldType == fieldType {
			return next, nil
		}
		mp = next
	}
	// we haven't found the index, so we need to create it
	next, err := mp.AddNext()
	if err != nil {
		return nil, fmt.Errorf("failed to add next meta page: %w", err)
	}
	metadata := &IndexMeta{}
	metadata.FieldName = name
	metadata.FieldType = fieldType
	buf, err := metadata.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return next, next.SetMetadata(buf)
}

// Synchronize will synchronize the index file with the data file.
// This is a convenience method and is equivalent to calling
// Synchronize() on the data handler itself.
func (i *IndexFile) Synchronize(df DataFile) error {
	return i.dataHandler.Synchronize(i, df)
}
