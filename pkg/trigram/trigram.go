package trigram

import (
	"golang.org/x/text/unicode/norm"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Trigram struct {
	Word   string
	Offset uint64
}

const N = 3

// BuildTrigram makes two passes
//
//	1 - splits by white space and keeps track of the positions
//	2 - performs a sliding window and builds trigrams

func normalizeToAscii(s string) (string, map[int]int) {
	ogOffsets := make(map[int]int)

	var b strings.Builder
	norm := norm.NFKD.String(s)

	additionalOffsets := 0

	newIndex := 0

	for i, r := range norm {
		if utf8.RuneLen(r) > 1 {
			additionalOffsets += utf8.RuneLen(r) - 1
		}

		if r <= unicode.MaxASCII {
			b.WriteRune(r)
			ogOffsets[newIndex] = i - additionalOffsets
			newIndex++
		}

	}
	return b.String(), ogOffsets
}

func BuildTrigram(phrase string) []Trigram {
	var trigrams []Trigram

	var words [][]int
	var currWord []int

	clean, ogOffsets := normalizeToAscii(phrase)

	runes := []rune(clean)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			currWord = append(currWord, i)
		} else if unicode.IsSpace(r) {
			if len(currWord) >= N {
				words = append(words, currWord)
			}
			currWord = []int{}
		}
	}

	if len(currWord) >= N {
		words = append(words, currWord)
	}

	for _, wOffsets := range words {
		for i := 0; i <= len(wOffsets)-N; i++ {

			var str string
			for j := i; j < i+N; j++ {
				str += string(runes[wOffsets[j]])
			}

			q := ogOffsets[wOffsets[i]]
			trigrams = append(trigrams, Trigram{Word: str, Offset: uint64(q)})

		}
	}

	return trigrams
}
