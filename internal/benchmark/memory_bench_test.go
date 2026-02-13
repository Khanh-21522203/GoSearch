package benchmark

import (
	"testing"

	"GoSearch/internal/engine"
	"GoSearch/internal/scoring"
)

func BenchmarkMemory_QueryExecution(b *testing.B) {
	docIDs, freqs := buildPostings(1000)
	scorer := scoring.NewBM25Scorer(10000, 25.0)
	idf := scorer.IDF(100)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		collector := engine.NewTopKCollector(10)
		for it.Next() {
			score := scorer.Score(it.Freq(), 20, idf)
			collector.Collect(it.DocID(), score)
		}
		_ = collector.Results()
	}
}

func BenchmarkMemory_PostingsIteration(b *testing.B) {
	docIDs, freqs := buildPostings(10000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		for it.Next() {
			_ = it.DocID()
			_ = it.Freq()
		}
	}
}

func BenchmarkMemory_Scoring(b *testing.B) {
	scorer := scoring.NewBM25Scorer(10000, 25.0)
	idf := scorer.IDF(100)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			_ = scorer.Score(uint32(1+j%10), uint32(10+j%50), idf)
		}
	}
}

func BenchmarkMemory_TopKCollection(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := engine.NewTopKCollector(10)
		for j := 0; j < 1000; j++ {
			c.Collect(uint32(j), float32(j))
		}
	}
}
