package benchmark

import (
	"testing"

	"GoSearch/internal/engine"
)

func buildPostings(count int) ([]uint32, []uint32) {
	docIDs := make([]uint32, count)
	freqs := make([]uint32, count)
	for i := 0; i < count; i++ {
		docIDs[i] = uint32(i * 2)
		freqs[i] = uint32(1 + i%5)
	}
	return docIDs, freqs
}

func BenchmarkQuery_PostingsIteration_1K(b *testing.B) {
	docIDs, freqs := buildPostings(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		for it.Next() {
			_ = it.DocID()
			_ = it.Freq()
		}
	}
}

func BenchmarkQuery_PostingsIteration_100K(b *testing.B) {
	docIDs, freqs := buildPostings(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		for it.Next() {
			_ = it.DocID()
		}
	}
}

func BenchmarkQuery_Conjunction_2Iterators(b *testing.B) {
	// Two overlapping postings lists.
	ids1 := make([]uint32, 10000)
	ids2 := make([]uint32, 10000)
	freqs := make([]uint32, 10000)
	for i := 0; i < 10000; i++ {
		ids1[i] = uint32(i)
		ids2[i] = uint32(i * 2)
		freqs[i] = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it1 := engine.NewSlicePostingsIterator(ids1, freqs)
		it2 := engine.NewSlicePostingsIterator(ids2, freqs)
		conj := engine.NewConjunctionIterator([]engine.PostingsIterator{it1, it2})
		for conj.Next() {
			_ = conj.DocID()
		}
	}
}

func BenchmarkQuery_Disjunction_2Iterators(b *testing.B) {
	ids1 := make([]uint32, 5000)
	ids2 := make([]uint32, 5000)
	freqs := make([]uint32, 5000)
	for i := 0; i < 5000; i++ {
		ids1[i] = uint32(i * 2)
		ids2[i] = uint32(i*2 + 1)
		freqs[i] = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it1 := engine.NewSlicePostingsIterator(ids1, freqs)
		it2 := engine.NewSlicePostingsIterator(ids2, freqs)
		disj := engine.NewDisjunctionIterator([]engine.PostingsIterator{it1, it2})
		for disj.Next() {
			_ = disj.DocID()
		}
	}
}

func BenchmarkQuery_TopKCollector(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := engine.NewTopKCollector(10)
		for j := 0; j < 10000; j++ {
			c.Collect(uint32(j), float32(j%1000))
		}
		_ = c.Results()
	}
}

func BenchmarkQuery_Advance(b *testing.B) {
	docIDs, freqs := buildPostings(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		for target := uint32(0); target < 200_000; target += 1000 {
			if !it.Advance(target) {
				break
			}
		}
	}
}
