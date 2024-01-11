package appendable

import (
	"bytes"
	"strings"
	"testing"
)

func TestAppendDataRowCSV(t *testing.T) {

	var mockCsv string = "word,length\nhowdy,5"
	var mockCsv2 string = "word,length\nhowdy,5\ngood lord,9"

	t.Run("generate index file", func(t *testing.T) {
		i, err := NewIndexFile(CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})

		if err != nil {
			t.Fatal(err)
		}

		buf := &bytes.Buffer{}

		if err := i.Serialize(buf); err != nil {
			t.Fatal(err)
		}

		_, err = ReadIndexFile(buf, CSVHandler{ReadSeeker: strings.NewReader(mockCsv)})
		if err != nil {
			t.Fatal(err)
		}

	})

}
