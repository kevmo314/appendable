package trigram

import (
	"fmt"
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

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}
	})

	t.Run("test special trigram", func(t *testing.T) {
		p := "h∫owd*y)__do "

		expected := [5]Trigram{
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

		incoming := BuildTrigram(p)

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
		expected := [2]Trigram{
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

		incoming := BuildTrigram(p)

		if !reflect.DeepEqual(incoming, expected[:]) {
			t.Fatalf("expected incoming and expected to be equal. \nExpected: %v\nGot: %v\n", expected, incoming)
		}

	})
}
