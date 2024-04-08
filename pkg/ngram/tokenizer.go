package ngram

import (
	"golang.org/x/text/unicode/norm"
	"hash/fnv"
	"math/rand"
	"strings"
	"unicode"
	"unicode/utf8"
)

// NgramTokenizer generates the following tokens with the lengths: 1, 2, 3.
// This is for two searching modes:
// By default, we'll use the 12gram, that is the min-gram: 1 and max-gram: 2.
// Also support trigrams, which have min-gram: 3, max-gram: 3.

type Token struct {
	Word   string
	Offset uint64
	Length uint32
}

const MIN_GRAM = 1

const MAX_GRAM = 3

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

func combineHashes(tokens []Token) int64 {
	h := fnv.New32a()
	for _, t := range tokens {
		h.Write([]byte(t.Word))
	}
	return int64(h.Sum32())
}

func Shuffle(tokens []Token) []Token {
	soup := make([]Token, len(tokens))
	copy(soup, tokens)

	seed := combineHashes(tokens)
	rand.Seed(seed)
	for i := len(tokens) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		soup[i], soup[j] = soup[j], soup[i]
	}

	return soup
}

func BuildNgram(phrase string) []Token {
	var ngramTokens []Token

	var words [][]int
	var currWord []int

	clean, ogOffsets := normalizeToAscii(phrase)

	runes := []rune(clean)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			currWord = append(currWord, i)
		} else if unicode.IsSpace(r) {
			if len(currWord) >= MIN_GRAM {
				words = append(words, currWord)
			}
			currWord = []int{}
		}
	}

	if len(currWord) >= MIN_GRAM {
		words = append(words, currWord)
	}

	for gl := MIN_GRAM; gl <= MAX_GRAM; gl++ {

		wsPadLen := MAX_GRAM - gl

		for _, wOffsets := range words {
			for i := 0; i <= len(wOffsets)-gl; i++ {

				var str string

				p := 0
				for j := i; j < i+gl; j++ {
					str += string(runes[wOffsets[j]])
					p = j
				}

				if wsPadLen > 0 {
					ws := strings.Repeat(" ", wsPadLen)
					str = ws + str
				}

				q := ogOffsets[wOffsets[i]]
				ngramTokens = append(ngramTokens, Token{
					Word:   strings.ToLower(str),
					Offset: uint64(q),
					Length: uint32(ogOffsets[wOffsets[p]] - q + 1),
				})

			}
		}
	}

	return ngramTokens
}
