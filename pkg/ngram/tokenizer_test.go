package ngram

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNgram(t *testing.T) {
	t.Run("test basic ngram", func(t *testing.T) {
		p := "wef"

		expected := [6]Token{
			{
				Word:   "  w",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "  e",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "  f",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   " we",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   " ef",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   "wef",
				Offset: 0,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test compound words", func(t *testing.T) {
		p := "how doe"

		expected := [12]Token{
			{
				Word:   "  h",
				Length: 1,
			},
			{
				Word:   "  o",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "  d",
				Offset: 4,
				Length: 1,
			},
			{
				Word:   "  o",
				Offset: 5,
				Length: 1,
			},
			{
				Word:   "  e",
				Offset: 6,
				Length: 1,
			},
			{
				Word:   " ho",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   " ow",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   " do",
				Offset: 4,
				Length: 2,
			},
			{
				Word:   " oe",
				Offset: 5,
				Length: 2,
			},
			{
				Word:   "how",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "doe",
				Offset: 4,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special ngram", func(t *testing.T) {
		p := "h∫owd*"

		expected := [9]Token{
			{
				Word:   "  h",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "  o",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 3,
				Length: 1,
			},
			{
				Word:   "  d",
				Offset: 4,
				Length: 1,
			},
			{
				Word:   " ho",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   " ow",
				Offset: 2,
				Length: 2,
			},
			{
				Word:   " wd",
				Offset: 3,
				Length: 2,
			},
			{
				Word:   "how",
				Offset: 0,
				Length: 4,
			},
			{
				Word:   "owd",
				Offset: 2,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special ngram", func(t *testing.T) {
		p := "h∫ow dow"

		expected := [12]Token{
			{
				Word:   "  h",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "  o",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 3,
				Length: 1,
			},
			{
				Word:   "  d",
				Offset: 5,
				Length: 1,
			},
			{
				Word:   "  o",
				Offset: 6,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 7,
				Length: 1,
			},
			{
				Word:   " ho",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   " ow",
				Offset: 2,
				Length: 2,
			},
			{
				Word:   " do",
				Offset: 5,
				Length: 2,
			},
			{
				Word:   " ow",
				Offset: 6,
				Length: 2,
			},
			{
				Word:   "how",
				Offset: 0,
				Length: 4,
			},
			{
				Word:   "dow",
				Offset: 5,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("cleans properly", func(t *testing.T) {
		p := [2]string{"h∫owd∫y dow", "café"}

		for _, str := range p {
			clean, _ := normalizeToAscii(str)
			fmt.Printf("clean: %v", clean)
		}
	})

	t.Run("test special case", func(t *testing.T) {
		p := "café"
		expected := [9]Token{
			{
				Word:   "  c",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "  a",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "  f",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "  e",
				Offset: 3,
				Length: 1,
			},
			{
				Word:   " ca",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   " af",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   " fe",
				Offset: 2,
				Length: 2,
			},
			{
				Word:   "caf",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "afe",
				Offset: 1,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test letter case", func(t *testing.T) {
		p := "NEW kWm"

		expected := [12]Token{
			{
				Word:   "  n",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "  e",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "  k",
				Offset: 4,
				Length: 1,
			},
			{
				Word:   "  w",
				Offset: 5,
				Length: 1,
			},
			{
				Word:   "  m",
				Offset: 6,
				Length: 1,
			},
			{
				Word:   " ne",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   " ew",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   " kw",
				Offset: 4,
				Length: 2,
			},
			{
				Word:   " wm",
				Offset: 5,
				Length: 2,
			},
			{
				Word:   "new",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "kwm",
				Offset: 4,
				Length: 3,
			},
		}

		incoming := BuildNgram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nE: %v\nG: %v\n", expected, incoming)
		}
	})
}
