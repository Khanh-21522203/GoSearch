package benchmark

import (
	"testing"

	"GoSearch/internal/analysis"
)

func BenchmarkAnalysis_Standard_Short(b *testing.B) {
	a := analysis.NewStandardAnalyzer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Analyze("field", "The Quick Brown Fox")
	}
}

func BenchmarkAnalysis_Standard_Long(b *testing.B) {
	a := analysis.NewStandardAnalyzer()
	text := "Full-text search is a technique for searching documents stored in a database. " +
		"It involves indexing the content of documents and building inverted indexes that map " +
		"terms to the documents containing them. Modern search engines use sophisticated ranking " +
		"algorithms like BM25 to estimate the relevance of documents to a given query."
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Analyze("field", text)
	}
}

func BenchmarkAnalysis_Whitespace(b *testing.B) {
	a := analysis.NewWhitespaceAnalyzer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Analyze("field", "The Quick Brown Fox Jumps Over The Lazy Dog")
	}
}

func BenchmarkAnalysis_Keyword(b *testing.B) {
	a := analysis.NewKeywordAnalyzer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Analyze("field", "exact-match-value")
	}
}
