package analysis

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// StandardAnalyzer tokenizes on Unicode word boundaries and lowercases tokens.
type StandardAnalyzer struct{}

// NewStandardAnalyzer creates a new StandardAnalyzer.
func NewStandardAnalyzer() *StandardAnalyzer {
	return &StandardAnalyzer{}
}

// Analyze tokenizes the input using Unicode word boundary detection and lowercasing.
func (a *StandardAnalyzer) Analyze(_ string, text string) []Token {
	var tokens []Token
	pos := 0
	i := 0

	for i < len(text) {
		// Skip non-word characters.
		r, size := utf8.DecodeRuneInString(text[i:])
		if !isWordRune(r) {
			i += size
			continue
		}

		// Collect word characters.
		start := i
		for i < len(text) {
			r, size = utf8.DecodeRuneInString(text[i:])
			if !isWordRune(r) {
				break
			}
			i += size
		}

		term := strings.ToLower(text[start:i])
		if term != "" {
			tokens = append(tokens, Token{
				Term:      term,
				Position:  pos,
				StartByte: start,
				EndByte:   i,
			})
			pos++
		}
	}

	return tokens
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
