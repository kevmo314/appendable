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

func TestWidthAllocation(t *testing.T) {

	type Truth struct {
		Type  FieldType
		Width uint16
	}

	t.Run("should correctly allocate the fixed width or else for a given type", func(t *testing.T) {

		ws := [8]Truth{
			{FieldTypeArray, 0},
			{FieldTypeBoolean, 2},
			{FieldTypeNull, 1},
			{FieldTypeFloat64, 9},
			{FieldTypeInt64, 9},
			{FieldTypeObject, 0},
			{FieldTypeString, 0},
			{FieldTypeUint64, 9},
		}

		for _, w := range ws {
			expected := w.Width
			input := DetermineType(w.Type)

			if expected != input {
				t.Errorf("For type: %v, expected: %v, got: %v", w.Type, expected, input)
			}
		}
	})
}
