package engine

// PostingsIterator iterates over a postings list in document ID order.
type PostingsIterator interface {
	// Next advances to the next document. Returns false when exhausted.
	Next() bool

	// DocID returns the current document ID. Valid only after Next() returns true.
	DocID() uint32

	// Freq returns the term frequency in the current document.
	Freq() uint32

	// Advance moves to the first document >= target. Returns false if no such document.
	Advance(target uint32) bool

	// Cost returns an estimate of remaining documents.
	Cost() int64
}

// SlicePostingsIterator is a simple in-memory PostingsIterator backed by slices.
type SlicePostingsIterator struct {
	docIDs []uint32
	freqs  []uint32
	pos    int
}

// NewSlicePostingsIterator creates a PostingsIterator from doc ID and frequency slices.
// Both slices must be the same length and docIDs must be sorted ascending.
func NewSlicePostingsIterator(docIDs, freqs []uint32) *SlicePostingsIterator {
	return &SlicePostingsIterator{
		docIDs: docIDs,
		freqs:  freqs,
		pos:    -1,
	}
}

func (it *SlicePostingsIterator) Next() bool {
	it.pos++
	return it.pos < len(it.docIDs)
}

func (it *SlicePostingsIterator) DocID() uint32 {
	return it.docIDs[it.pos]
}

func (it *SlicePostingsIterator) Freq() uint32 {
	if it.freqs == nil || it.pos >= len(it.freqs) {
		return 1
	}
	return it.freqs[it.pos]
}

func (it *SlicePostingsIterator) Advance(target uint32) bool {
	// If already positioned at or past target, return true.
	if it.pos >= 0 && it.pos < len(it.docIDs) && it.docIDs[it.pos] >= target {
		return true
	}
	for it.pos+1 < len(it.docIDs) {
		it.pos++
		if it.docIDs[it.pos] >= target {
			return true
		}
	}
	it.pos = len(it.docIDs)
	return false
}

func (it *SlicePostingsIterator) Cost() int64 {
	remaining := len(it.docIDs) - it.pos - 1
	if remaining < 0 {
		return 0
	}
	return int64(remaining)
}
