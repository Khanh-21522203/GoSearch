package engine

import "container/heap"

// DisjunctionIterator implements OR logic over multiple PostingsIterators.
// It uses a min-heap to merge iterators in document ID order.
type DisjunctionIterator struct {
	h       iterHeap
	current uint32
}

// NewDisjunctionIterator creates an OR iterator over the given children.
func NewDisjunctionIterator(children []PostingsIterator) *DisjunctionIterator {
	d := &DisjunctionIterator{}

	// Initialize heap with all iterators that have at least one doc.
	for _, child := range children {
		if child.Next() {
			d.h = append(d.h, child)
		}
	}
	heap.Init(&d.h)

	return d
}

func (d *DisjunctionIterator) Next() bool {
	if len(d.h) == 0 {
		return false
	}

	// Pop the minimum doc ID.
	d.current = d.h[0].DocID()

	// Advance all iterators at the current doc ID.
	for len(d.h) > 0 && d.h[0].DocID() == d.current {
		top := d.h[0]
		if top.Next() {
			heap.Fix(&d.h, 0)
		} else {
			heap.Pop(&d.h)
		}
	}

	return true
}

func (d *DisjunctionIterator) DocID() uint32 {
	return d.current
}

func (d *DisjunctionIterator) Freq() uint32 {
	return 1 // Approximate for OR.
}

func (d *DisjunctionIterator) Advance(target uint32) bool {
	// Advance all iterators past target.
	for len(d.h) > 0 && d.h[0].DocID() < target {
		top := d.h[0]
		if top.Advance(target) {
			heap.Fix(&d.h, 0)
		} else {
			heap.Pop(&d.h)
		}
	}

	if len(d.h) == 0 {
		return false
	}

	d.current = d.h[0].DocID()
	return true
}

func (d *DisjunctionIterator) Cost() int64 {
	var total int64
	for _, it := range d.h {
		total += it.Cost()
	}
	return total
}

// iterHeap is a min-heap of PostingsIterators ordered by current DocID.
type iterHeap []PostingsIterator

func (h iterHeap) Len() int            { return len(h) }
func (h iterHeap) Less(i, j int) bool   { return h[i].DocID() < h[j].DocID() }
func (h iterHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)          { *h = append(*h, x.(PostingsIterator)) }
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
