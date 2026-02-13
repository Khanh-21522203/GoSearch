package engine

import "container/heap"

// ScoredDoc represents a document with its score.
type ScoredDoc struct {
	DocID uint32
	Score float32
}

// TopKCollector collects the top-K scoring documents using a min-heap.
type TopKCollector struct {
	k        int
	h        scoreHeap
	minScore float32
}

// NewTopKCollector creates a collector for the top K documents.
func NewTopKCollector(k int) *TopKCollector {
	if k <= 0 {
		k = 10
	}
	return &TopKCollector{
		k: k,
		h: make(scoreHeap, 0, k),
	}
}

// Collect adds a document to the collector if it qualifies for top-K.
func (c *TopKCollector) Collect(docID uint32, score float32) {
	if c.h.Len() < c.k {
		heap.Push(&c.h, ScoredDoc{DocID: docID, Score: score})
		if c.h.Len() == c.k {
			c.minScore = c.h[0].Score
		}
	} else if score > c.minScore {
		c.h[0] = ScoredDoc{DocID: docID, Score: score}
		heap.Fix(&c.h, 0)
		c.minScore = c.h[0].Score
	}
}

// MinScore returns the current minimum score in the collector.
// Returns 0 if fewer than K documents have been collected.
func (c *TopKCollector) MinScore() float32 {
	return c.minScore
}

// Len returns the number of documents collected so far.
func (c *TopKCollector) Len() int {
	return c.h.Len()
}

// Results returns the collected documents sorted descending by score.
func (c *TopKCollector) Results() []ScoredDoc {
	result := make([]ScoredDoc, c.h.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(&c.h).(ScoredDoc)
	}
	return result
}

// scoreHeap is a min-heap of ScoredDoc ordered by score.
type scoreHeap []ScoredDoc

func (h scoreHeap) Len() int            { return len(h) }
func (h scoreHeap) Less(i, j int) bool   { return h[i].Score < h[j].Score }
func (h scoreHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *scoreHeap) Push(x any)          { *h = append(*h, x.(ScoredDoc)) }
func (h *scoreHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
