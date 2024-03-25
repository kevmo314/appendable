package trigram

import (
	"unicode"
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
func BuildTrigram(phrase string) []Trigram {
	var trigrams []Trigram

	var words [][]int
	var currWord []int

	runes := []rune(phrase)
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

			trigrams = append(trigrams, Trigram{Word: str, Offset: uint64(wOffsets[i])})

		}
	}

	return trigrams
}
