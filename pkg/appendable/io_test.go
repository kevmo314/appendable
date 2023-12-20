package appendable

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestDetermineDataHandler(t *testing.T) {

	t.Run("unrecognized file", func(t *testing.T) {
		filepath := "wef"

		_, err := DetermineDataHandler(filepath)

		if err == nil {
			t.Errorf("DetermineDataHandler() did not error, expected err")
		}
	})

	t.Run("jsonl file", func(t *testing.T) {
		filePath := "examplewef.jsonl"

		handler, err := DetermineDataHandler(filePath)

		if err != nil {
			t.Errorf("DetermineDataHandler() returned an unexpected error: %v", err)
		}

		_, isJSONLHandler := handler.(JSONLHandler)

		if !isJSONLHandler {
			t.Errorf("DetermineDataHandler() returned incorrect handler type, expected JSONLHandler")
		}
	})
}

func TestReadIndexFile(t *testing.T) {
	t.Run("empty index file", func(t *testing.T) {
		if _, err := ReadIndexFile(strings.NewReader(""), strings.NewReader(""), JSONLHandler{}); !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF, got %v", err)
		}
	})
}
