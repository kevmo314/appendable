package appendable

import (
	"errors"
	"io"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestReadIndexFile(t *testing.T) {

	logger, err := zap.NewDevelopment()

	if err != nil {
		panic("cannot initialize zap logger: " + err.Error())
	}

	defer logger.Sync()
	sugar := logger.Sugar()

	t.Run("empty index file", func(t *testing.T) {
		if _, err := ReadIndexFile(strings.NewReader(""), JSONLHandler{ReadSeeker: strings.NewReader("")}, sugar); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}
