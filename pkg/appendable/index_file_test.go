package appendable

import (
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
)

type FormatHandler struct{ ReturnsFormat Format }

func (f FormatHandler) Format() Format {
	return f.ReturnsFormat
}

func (f FormatHandler) Synchronize(f1 *IndexFile, df []byte) error {
	return nil
}

func (f FormatHandler) Parse(data []byte) []byte {
	return nil
}

func TestIndexFile(t *testing.T) {
	t.Run("validate metadata throws error if format doesn't match on second read", func(t *testing.T) {
		f := buftest.NewSeekableBuffer()

		if _, err := NewIndexFile(f, &FormatHandler{ReturnsFormat: Format(1)}); err != nil {
			t.Fatal(err)
		}

		// try creating a new index file with a different format
		if _, err := NewIndexFile(f, &FormatHandler{ReturnsFormat: Format(2)}); err == nil {
			t.Fatal("expected error")
		}
	})
}
