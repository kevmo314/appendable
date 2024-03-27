package trigram

import (
	"reflect"
	"testing"
)

func TestTrigram(t *testing.T) {
	t.Run("test basic trigram", func(t *testing.T) {
		p := "howdydo"

		expected := [5]Trigram{
			{
				Word:   "how",
				Offset: 0,
			},
			{
				Word:   "owd",
				Offset: 1,
			},
			{
				Word:   "wdy",
				Offset: 2,
			},
			{
				Word:   "dyd",
				Offset: 3,
			},
			{
				Word:   "ydo",
				Offset: 4,
			},
		}

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test compound words", func(t *testing.T) {
		p := "howdy do"

		expected := [3]Trigram{
			{
				Word:   "how",
				Offset: 0,
			},
			{
				Word:   "owd",
				Offset: 1,
			},
			{
				Word:   "wdy",
				Offset: 2,
			},
		}

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special trigram", func(t *testing.T) {
		p := "h∫owd*y)__do "

		expected := [3]Trigram{
			{
				Word:   "how",
				Offset: 0,
			},
			{
				Word:   "owd",
				Offset: 2,
			},
			{
				Word:   "wdy",
				Offset: 3,
			},
		}

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special trigram", func(t *testing.T) {
		p := "h∫owd∫y dow "

		expected := [4]Trigram{
			{
				Word:   "how",
				Offset: 0,
			},
			{
				Word:   "owd",
				Offset: 2,
			},
			{
				Word:   "wdy",
				Offset: 3,
			},
			{
				Word:   "dow",
				Offset: 8,
			},
		}

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special case", func(t *testing.T) {
		p := "café"
		expected := [2]Trigram{
			{
				Word:   "caf",
				Offset: 0,
			},
			{
				Word:   "afe",
				Offset: 1,
			},
		}

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}

	})
}
