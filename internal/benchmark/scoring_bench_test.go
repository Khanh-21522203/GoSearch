package benchmark

import (
	"testing"

	"GoSearch/internal/scoring"
)

func BenchmarkScoring_BM25_SingleTerm(b *testing.B) {
	s := scoring.NewBM25Scorer(100000, 25.0)
	idf := s.IDF(500)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Score(3, 20, idf)
	}
}

func BenchmarkScoring_BM25_IDF(b *testing.B) {
	s := scoring.NewBM25Scorer(100000, 25.0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.IDF(500)
	}
}

func BenchmarkScoring_BM25_MultiTerm(b *testing.B) {
	s := scoring.NewBM25Scorer(100000, 25.0)
	terms := []scoring.QueryTerm{
		{Term: "search", TermFreq: 3, DocFreq: 500, Boost: 1.0},
		{Term: "engine", TermFreq: 1, DocFreq: 200, Boost: 1.0},
		{Term: "fast", TermFreq: 2, DocFreq: 1000, Boost: 1.5},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScoreMultiTerm(terms, 25)
	}
}

func BenchmarkScoring_BM25_Explain(b *testing.B) {
	s := scoring.NewBM25Scorer(100000, 25.0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Explain("title", "search", 3, 20, 500)
	}
}
