package engine

import (
	"testing"
	"time"
)

// --- PostingsIterator Tests ---

func TestSlicePostingsIterator_Basic(t *testing.T) {
	it := NewSlicePostingsIterator([]uint32{1, 3, 5, 7}, []uint32{2, 1, 3, 1})

	var docs []uint32
	for it.Next() {
		docs = append(docs, it.DocID())
	}
	if len(docs) != 4 {
		t.Fatalf("expected 4 docs, got %d", len(docs))
	}
	expected := []uint32{1, 3, 5, 7}
	for i, d := range docs {
		if d != expected[i] {
			t.Errorf("doc[%d] = %d, want %d", i, d, expected[i])
		}
	}
}

func TestSlicePostingsIterator_Advance(t *testing.T) {
	it := NewSlicePostingsIterator([]uint32{1, 3, 5, 7, 9}, []uint32{1, 1, 1, 1, 1})

	if !it.Advance(4) {
		t.Fatal("Advance(4) should find doc >= 4")
	}
	if it.DocID() != 5 {
		t.Errorf("DocID = %d, want 5", it.DocID())
	}

	if !it.Advance(7) {
		t.Fatal("Advance(7) should find doc 7")
	}
	if it.DocID() != 7 {
		t.Errorf("DocID = %d, want 7", it.DocID())
	}

	if it.Advance(100) {
		t.Error("Advance(100) should return false")
	}
}

func TestSlicePostingsIterator_Empty(t *testing.T) {
	it := NewSlicePostingsIterator(nil, nil)
	if it.Next() {
		t.Error("empty iterator should return false")
	}
}

func TestSlicePostingsIterator_Freq(t *testing.T) {
	it := NewSlicePostingsIterator([]uint32{1, 2}, []uint32{5, 10})
	it.Next()
	if it.Freq() != 5 {
		t.Errorf("Freq = %d, want 5", it.Freq())
	}
	it.Next()
	if it.Freq() != 10 {
		t.Errorf("Freq = %d, want 10", it.Freq())
	}
}

func TestSlicePostingsIterator_Cost(t *testing.T) {
	it := NewSlicePostingsIterator([]uint32{1, 2, 3, 4, 5}, nil)
	if it.Cost() != 4 { // pos=-1, remaining=5-(-1)-1=5... actually 5 total
		// Before any Next(), pos=-1, remaining = 5 - (-1) - 1 = 5
	}
	it.Next()
	if it.Cost() != 4 {
		t.Errorf("Cost = %d, want 4", it.Cost())
	}
}

// --- Conjunction Tests ---

func TestConjunctionIterator_Basic(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 2, 3, 5, 7}, []uint32{1, 1, 1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{2, 3, 4, 5, 8}, []uint32{1, 1, 1, 1, 1})

	conj := NewConjunctionIterator([]PostingsIterator{a, b})

	var docs []uint32
	for conj.Next() {
		docs = append(docs, conj.DocID())
	}

	expected := []uint32{2, 3, 5}
	if len(docs) != len(expected) {
		t.Fatalf("expected %d docs, got %d: %v", len(expected), len(docs), docs)
	}
	for i, d := range docs {
		if d != expected[i] {
			t.Errorf("doc[%d] = %d, want %d", i, d, expected[i])
		}
	}
}

func TestConjunctionIterator_NoOverlap(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 3, 5}, []uint32{1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{2, 4, 6}, []uint32{1, 1, 1})

	conj := NewConjunctionIterator([]PostingsIterator{a, b})
	if conj.Next() {
		t.Error("expected no results for non-overlapping iterators")
	}
}

func TestConjunctionIterator_ThreeWay(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 2, 3, 4, 5}, []uint32{1, 1, 1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{2, 3, 5}, []uint32{1, 1, 1})
	c := NewSlicePostingsIterator([]uint32{3, 5, 7}, []uint32{1, 1, 1})

	conj := NewConjunctionIterator([]PostingsIterator{a, b, c})

	var docs []uint32
	for conj.Next() {
		docs = append(docs, conj.DocID())
	}

	expected := []uint32{3, 5}
	if len(docs) != len(expected) {
		t.Fatalf("expected %d docs, got %d: %v", len(expected), len(docs), docs)
	}
}

func TestConjunctionIterator_Advance(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 3, 5, 7, 9}, []uint32{1, 1, 1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{1, 3, 5, 7, 9}, []uint32{1, 1, 1, 1, 1})

	conj := NewConjunctionIterator([]PostingsIterator{a, b})
	if !conj.Advance(5) {
		t.Fatal("Advance(5) should succeed")
	}
	if conj.DocID() != 5 {
		t.Errorf("DocID = %d, want 5", conj.DocID())
	}
}

// --- Disjunction Tests ---

func TestDisjunctionIterator_Basic(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 3, 5}, []uint32{1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{2, 3, 6}, []uint32{1, 1, 1})

	disj := NewDisjunctionIterator([]PostingsIterator{a, b})

	var docs []uint32
	for disj.Next() {
		docs = append(docs, disj.DocID())
	}

	expected := []uint32{1, 2, 3, 5, 6}
	if len(docs) != len(expected) {
		t.Fatalf("expected %d docs, got %d: %v", len(expected), len(docs), docs)
	}
	for i, d := range docs {
		if d != expected[i] {
			t.Errorf("doc[%d] = %d, want %d", i, d, expected[i])
		}
	}
}

func TestDisjunctionIterator_Duplicates(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 2, 3}, []uint32{1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{1, 2, 3}, []uint32{1, 1, 1})

	disj := NewDisjunctionIterator([]PostingsIterator{a, b})

	var docs []uint32
	for disj.Next() {
		docs = append(docs, disj.DocID())
	}

	// Should deduplicate.
	expected := []uint32{1, 2, 3}
	if len(docs) != len(expected) {
		t.Fatalf("expected %d docs, got %d: %v", len(expected), len(docs), docs)
	}
}

func TestDisjunctionIterator_Empty(t *testing.T) {
	disj := NewDisjunctionIterator(nil)
	if disj.Next() {
		t.Error("empty disjunction should return false")
	}
}

func TestDisjunctionIterator_Advance(t *testing.T) {
	a := NewSlicePostingsIterator([]uint32{1, 5, 10}, []uint32{1, 1, 1})
	b := NewSlicePostingsIterator([]uint32{3, 7, 10}, []uint32{1, 1, 1})

	disj := NewDisjunctionIterator([]PostingsIterator{a, b})
	if !disj.Advance(6) {
		t.Fatal("Advance(6) should succeed")
	}
	if disj.DocID() != 7 {
		t.Errorf("DocID = %d, want 7", disj.DocID())
	}
}

// --- TopKCollector Tests ---

func TestTopKCollector_Basic(t *testing.T) {
	c := NewTopKCollector(3)

	c.Collect(1, 1.0)
	c.Collect(2, 3.0)
	c.Collect(3, 2.0)
	c.Collect(4, 5.0)
	c.Collect(5, 4.0)

	results := c.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be sorted descending: 5.0, 4.0, 3.0
	expectedScores := []float32{5.0, 4.0, 3.0}
	for i, r := range results {
		if r.Score != expectedScores[i] {
			t.Errorf("result[%d].Score = %f, want %f", i, r.Score, expectedScores[i])
		}
	}
}

func TestTopKCollector_LessThanK(t *testing.T) {
	c := NewTopKCollector(10)
	c.Collect(1, 1.0)
	c.Collect(2, 2.0)

	results := c.Results()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestTopKCollector_Empty(t *testing.T) {
	c := NewTopKCollector(10)
	results := c.Results()
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestTopKCollector_MinScore(t *testing.T) {
	c := NewTopKCollector(2)
	c.Collect(1, 5.0)
	if c.MinScore() != 0 {
		t.Errorf("MinScore before full = %f, want 0", c.MinScore())
	}

	c.Collect(2, 3.0)
	if c.MinScore() != 3.0 {
		t.Errorf("MinScore = %f, want 3.0", c.MinScore())
	}

	c.Collect(3, 10.0)
	if c.MinScore() != 5.0 {
		t.Errorf("MinScore after replacement = %f, want 5.0", c.MinScore())
	}
}

// --- ExecutionContext Tests ---

func TestExecutionContext_StateLimitExceeded(t *testing.T) {
	ctx := NewExecutionContext(time.Minute, 5, 1000)
	ctx.StatesVisited = 5
	err := ctx.CheckLimits()
	if err != ErrStateLimitExceeded {
		t.Errorf("expected ErrStateLimitExceeded, got %v", err)
	}
}

func TestExecutionContext_MatchLimitExceeded(t *testing.T) {
	ctx := NewExecutionContext(time.Minute, 10000, 5)
	ctx.TermsMatched = 5
	err := ctx.CheckLimits()
	if err != ErrMatchLimitExceeded {
		t.Errorf("expected ErrMatchLimitExceeded, got %v", err)
	}
}

func TestExecutionContext_NoLimitExceeded(t *testing.T) {
	ctx := NewExecutionContext(time.Minute, 10000, 1000)
	ctx.StatesVisited = 1
	ctx.TermsMatched = 1
	err := ctx.CheckLimits()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecutionContext_Timeout(t *testing.T) {
	ctx := NewExecutionContext(1*time.Nanosecond, 10000, 1000)
	time.Sleep(time.Millisecond)
	// Force the check interval to trigger.
	ctx.checkCounter = ctx.checkInterval - 1
	err := ctx.CheckLimits()
	if err != ErrQueryTimeout {
		t.Errorf("expected ErrQueryTimeout, got %v", err)
	}
}
