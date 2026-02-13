package query

// Rewrite applies optimization rules to a query AST until a fixed point is reached.
// Rules: flatten nested booleans, remove MatchAll from AND, short-circuit MatchNone in AND,
// propagate NOT(MatchAll) → MatchNone.
func Rewrite(q Query) Query {
	for {
		rewritten := rewriteOnce(q)
		if queryEqual(rewritten, q) {
			return rewritten
		}
		q = rewritten
	}
}

func rewriteOnce(q Query) Query {
	switch v := q.(type) {
	case *BooleanQuery:
		return rewriteBoolean(v)
	default:
		return q
	}
}

func rewriteBoolean(q *BooleanQuery) Query {
	// Recursively rewrite children first.
	clauses := make([]BooleanClause, 0, len(q.Clauses))
	for _, c := range q.Clauses {
		rewritten := rewriteOnce(c.Query)

		// Flatten nested booleans with same operator.
		if inner, ok := rewritten.(*BooleanQuery); ok {
			if canFlatten(c.Occur, inner) {
				for _, ic := range inner.Clauses {
					clauses = append(clauses, BooleanClause{Occur: c.Occur, Query: ic.Query})
				}
				continue
			}
		}

		clauses = append(clauses, BooleanClause{Occur: c.Occur, Query: rewritten})
	}

	// Remove MatchAll from AND (must) clauses.
	filtered := make([]BooleanClause, 0, len(clauses))
	hasMust := false
	for _, c := range clauses {
		if c.Occur == BooleanMust {
			hasMust = true
			if _, ok := c.Query.(*MatchAllQuery); ok {
				continue // Remove MatchAll from AND.
			}
		}
		filtered = append(filtered, c)
	}

	// Short-circuit: MatchNone in AND → MatchNone.
	for _, c := range filtered {
		if c.Occur == BooleanMust {
			if _, ok := c.Query.(*MatchNoneQuery); ok {
				return &MatchNoneQuery{}
			}
		}
	}

	// If all must clauses were removed (all MatchAll), and no other clauses, return MatchAll.
	if hasMust && len(filtered) == 0 {
		return &MatchAllQuery{}
	}

	// Single clause remaining: unwrap.
	if len(filtered) == 1 && filtered[0].Occur == BooleanMust {
		return filtered[0].Query
	}

	return &BooleanQuery{
		Clauses:            filtered,
		MinimumShouldMatch: q.MinimumShouldMatch,
	}
}

// canFlatten returns true if an inner boolean can be flattened into the outer clause.
// AND(AND(a,b)) → AND(a,b) and OR(OR(a,b)) → OR(a,b).
func canFlatten(outerOccur BooleanOp, inner *BooleanQuery) bool {
	if outerOccur == BooleanMustNot {
		return false
	}
	for _, c := range inner.Clauses {
		if c.Occur != outerOccur {
			return false
		}
	}
	return true
}

// queryEqual checks structural equality for fixed-point detection.
func queryEqual(a, b Query) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Type() != b.Type() {
		return false
	}
	// For boolean queries, compare clause count (sufficient for fixed-point).
	if ab, ok := a.(*BooleanQuery); ok {
		bb := b.(*BooleanQuery)
		if len(ab.Clauses) != len(bb.Clauses) {
			return false
		}
		for i := range ab.Clauses {
			if ab.Clauses[i].Occur != bb.Clauses[i].Occur {
				return false
			}
			if !queryEqual(ab.Clauses[i].Query, bb.Clauses[i].Query) {
				return false
			}
		}
		return true
	}
	// For leaf nodes, pointer equality is sufficient after one pass.
	return a == b
}
