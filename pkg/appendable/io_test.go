package appendable

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadIndexFile(t *testing.T) {
	t.Run("empty index file", func(t *testing.T) {
		if _, err := ReadIndexFile(strings.NewReader(""), strings.NewReader("")); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}
