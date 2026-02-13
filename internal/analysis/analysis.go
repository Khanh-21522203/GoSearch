package analysis

// Token represents a single token produced by an analyzer.
type Token struct {
	Term      string
	Position  int
	StartByte int
	EndByte   int
}

// Analyzer processes text into a stream of tokens.
// Implementations MUST be safe for reuse across documents after Reset().
type Analyzer interface {
	// Analyze tokenizes the input text and returns tokens with positions.
	Analyze(field string, text string) []Token
}
