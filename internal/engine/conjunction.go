package engine

import "sort"

// ConjunctionIterator implements AND logic over multiple PostingsIterators.
// It uses the lowest-cost iterator as the lead and advances all others to alignment.
type ConjunctionIterator struct {
	children []PostingsIterator
	lead     PostingsIterator
	current  uint32
}

// NewConjunctionIterator creates an AND iterator over the given children.
// Children must not be empty.
func NewConjunctionIterator(children []PostingsIterator) *ConjunctionIterator {
	// Sort by cost ascending so the cheapest iterator leads.
	sorted := make([]PostingsIterator, len(children))
	copy(sorted, children)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Cost() < sorted[j].Cost()
	})

	return &ConjunctionIterator{
		children: sorted,
		lead:     sorted[0],
	}
}

func (c *ConjunctionIterator) Next() bool {
	if !c.lead.Next() {
		return false
	}
	return c.align(c.lead.DocID())
}

func (c *ConjunctionIterator) DocID() uint32 {
	return c.current
}

func (c *ConjunctionIterator) Freq() uint32 {
	// Return the lead's frequency.
	return c.lead.Freq()
}

func (c *ConjunctionIterator) Advance(target uint32) bool {
	if !c.lead.Advance(target) {
		return false
	}
	return c.align(c.lead.DocID())
}

func (c *ConjunctionIterator) Cost() int64 {
	return c.lead.Cost()
}

// align advances all iterators until they all point to the same document.
func (c *ConjunctionIterator) align(target uint32) bool {
	for {
		allAligned := true
		for _, child := range c.children {
			if child == c.lead {
				continue
			}
			if !child.Advance(target) {
				return false
			}
			if child.DocID() > target {
				target = child.DocID()
				if !c.lead.Advance(target) {
					return false
				}
				// Lead may have landed past target.
				target = c.lead.DocID()
				allAligned = false
				break
			}
		}
		if allAligned {
			c.current = target
			return true
		}
	}
}
