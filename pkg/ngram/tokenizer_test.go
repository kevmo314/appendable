package ngram

import (
	"reflect"
	"testing"
)

func TestUnigram(t *testing.T) {
	t.Run("test basic", func(t *testing.T) {
		p := "wef"

		expected := [3]Token{
			{
				Word:   "w",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "e",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "f",
				Offset: 2,
				Length: 1,
			},
		}

		incoming := BuildNgram(p, 1)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test compound words", func(t *testing.T) {
		p := "h do"

		expected := [3]Token{
			{
				Word:   "h",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "d",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "o",
				Offset: 3,
				Length: 1,
			},
		}

		incoming := BuildNgram(p, 1)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special", func(t *testing.T) {
		p := "h∫owd*y)__do "

		expected := [7]Token{
			{
				Word:   "h",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "o",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "w",
				Offset: 3,
				Length: 1,
			},
			{
				Word:   "d",
				Offset: 4,
				Length: 1,
			},
			{
				Word:   "y",
				Offset: 6,
				Length: 1,
			},
			{
				Word:   "d",
				Offset: 10,
				Length: 1,
			},
			{
				Word:   "o",
				Offset: 11,
				Length: 1,
			},
		}

		incoming := BuildNgram(p, 1)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special case", func(t *testing.T) {
		p := "café"
		expected := [4]Token{
			{
				Word:   "c",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "a",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "f",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "e",
				Offset: 3,
				Length: 1,
			},
		}

		incoming := BuildNgram(p, 1)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test letter case", func(t *testing.T) {
		p := "NEW kWm"

		expected := [6]Token{
			{
				Word:   "n",
				Offset: 0,
				Length: 1,
			},
			{
				Word:   "e",
				Offset: 1,
				Length: 1,
			},
			{
				Word:   "w",
				Offset: 2,
				Length: 1,
			},
			{
				Word:   "k",
				Offset: 4,
				Length: 1,
			},
			{
				Word:   "w",
				Offset: 5,
				Length: 1,
			},
			{
				Word:   "m",
				Offset: 6,
				Length: 1,
			},
		}

		incoming := BuildNgram(p, 1)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nE: %v\nG: %v\n", expected, incoming)
		}
	})
}

func TestBigram(t *testing.T) {
	t.Run("test basic", func(t *testing.T) {
		p := "wef"

		expected := [2]Token{
			{
				Word:   "we",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   "ef",
				Offset: 1,
				Length: 2,
			},
		}

		incoming := BuildNgram(p, 2)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test compound words", func(t *testing.T) {
		p := "howdy do"

		expected := [5]Token{
			{
				Word:   "ho",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   "ow",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   "wd",
				Offset: 2,
				Length: 2,
			},
			{
				Word:   "dy",
				Offset: 3,
				Length: 2,
			},
			{
				Word:   "do",
				Offset: 6,
				Length: 2,
			},
		}

		incoming := BuildNgram(p, 2)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special", func(t *testing.T) {
		p := "h∫owd*y)__do "

		expected := [6]Token{
			{
				Word:   "ho",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "ow",
				Offset: 2,
				Length: 2,
			},
			{
				Word:   "wd",
				Offset: 3,
				Length: 2,
			},
			{
				Word:   "dy",
				Offset: 4,
				Length: 3,
			},
			{
				Word:   "yd",
				Offset: 6,
				Length: 5,
			},
			{
				Word:   "do",
				Offset: 10,
				Length: 2,
			},
		}

		incoming := BuildNgram(p, 2)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nE: %v\nG: %v\n", expected, incoming)
		}
	})

	t.Run("test special case", func(t *testing.T) {
		p := "café"
		expected := [3]Token{
			{
				Word:   "ca",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   "af",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   "fe",
				Offset: 2,
				Length: 2,
			},
		}

		incoming := BuildNgram(p, 2)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test letter case", func(t *testing.T) {
		p := "NEW kWm"

		expected := [4]Token{
			{
				Word:   "ne",
				Offset: 0,
				Length: 2,
			},
			{
				Word:   "ew",
				Offset: 1,
				Length: 2,
			},
			{
				Word:   "kw",
				Offset: 4,
				Length: 2,
			},
			{
				Word:   "wm",
				Offset: 5,
				Length: 2,
			},
		}

		incoming := BuildNgram(p, 2)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nE: %v\nG: %v\n", expected, incoming)
		}
	})
}

func TestTrigram(t *testing.T) {
	t.Run("test basic", func(t *testing.T) {
		p := "howdydo"

		expected := [5]Token{
			{
				Word:   "how",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "owd",
				Offset: 1,
				Length: 3,
			},
			{
				Word:   "wdy",
				Offset: 2,
				Length: 3,
			},
			{
				Word:   "dyd",
				Offset: 3,
				Length: 3,
			},
			{
				Word:   "ydo",
				Offset: 4,
				Length: 3,
			},
		}

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test compound words", func(t *testing.T) {
		p := "howdy do"

		expected := [3]Token{
			{
				Word:   "how",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "owd",
				Offset: 1,
				Length: 3,
			},
			{
				Word:   "wdy",
				Offset: 2,
				Length: 3,
			},
		}

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special", func(t *testing.T) {
		p := "h∫owd*y)__do "

		expected := [5]Token{
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
			{
				Word:   "wdy",
				Offset: 3,
				Length: 4,
			},
			{
				Word:   "dyd",
				Offset: 4,
				Length: 7,
			},
			{
				Word:   "ydo",
				Offset: 6,
				Length: 6,
			},
		}

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special", func(t *testing.T) {
		p := "h∫owd∫y dow "

		expected := [4]Token{
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
			{
				Word:   "wdy",
				Offset: 3,
				Length: 4,
			},
			{
				Word:   "dow",
				Offset: 8,
				Length: 3,
			},
		}

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special case", func(t *testing.T) {
		p := "café"
		expected := [2]Token{
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

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test letter case", func(t *testing.T) {
		p := "NEW ROOF kWm"

		expected := [4]Token{
			{
				Word:   "new",
				Offset: 0,
				Length: 3,
			},
			{
				Word:   "roo",
				Offset: 4,
				Length: 3,
			},
			{
				Word:   "oof",
				Offset: 5,
				Length: 3,
			},
			{
				Word:   "kwm",
				Offset: 9,
				Length: 3,
			},
		}

		incoming := BuildNgram(p, 3)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nE: %v\nG: %v\n", expected, incoming)
		}
	})
}
