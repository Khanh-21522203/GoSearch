package analysis

// KeywordAnalyzer passes the entire input as a single token with no tokenization.
type KeywordAnalyzer struct{}

// NewKeywordAnalyzer creates a new KeywordAnalyzer.
func NewKeywordAnalyzer() *KeywordAnalyzer {
	return &KeywordAnalyzer{}
}

// Analyze returns the entire input as a single token.
func (a *KeywordAnalyzer) Analyze(_ string, text string) []Token {
	if text == "" {
		return nil
	}
	return []Token{
		{
			Term:      text,
			Position:  0,
			StartByte: 0,
			EndByte:   len(text),
		},
	}
}
