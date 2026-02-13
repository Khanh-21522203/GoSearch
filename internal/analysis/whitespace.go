package analysis

import "strings"

// WhitespaceAnalyzer splits text on whitespace without any normalization.
type WhitespaceAnalyzer struct{}

// NewWhitespaceAnalyzer creates a new WhitespaceAnalyzer.
func NewWhitespaceAnalyzer() *WhitespaceAnalyzer {
	return &WhitespaceAnalyzer{}
}

// Analyze splits the input on whitespace, preserving case.
func (a *WhitespaceAnalyzer) Analyze(_ string, text string) []Token {
	fields := strings.Fields(text)
	tokens := make([]Token, 0, len(fields))

	pos := 0
	searchFrom := 0
	for _, f := range fields {
		idx := strings.Index(text[searchFrom:], f)
		startByte := searchFrom + idx
		endByte := startByte + len(f)

		tokens = append(tokens, Token{
			Term:      f,
			Position:  pos,
			StartByte: startByte,
			EndByte:   endByte,
		})
		pos++
		searchFrom = endByte
	}

	return tokens
}
