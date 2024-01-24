package appendable

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestReadIndexFile(t *testing.T) {

	var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

	t.Run("empty index file", func(t *testing.T) {
		if _, err := ReadIndexFile(strings.NewReader(""), JSONLHandler{ReadSeeker: strings.NewReader("")}, logger); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}
