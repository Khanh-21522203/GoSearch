package query

import (
	"testing"
)

func TestQueryTypes(t *testing.T) {
	tests := []struct {
		name string
		q    Query
		want QueryType
	}{
		{"TermQuery", &TermQuery{Field: "title", Term: "hello"}, QueryTypeTerm},
		{"BooleanQuery", &BooleanQuery{}, QueryTypeBoolean},
		{"PrefixQuery", &PrefixQuery{Field: "title", Prefix: "hel"}, QueryTypePrefix},
		{"WildcardQuery", &WildcardQuery{Field: "title", Pattern: "h*o"}, QueryTypeWildcard},
		{"RegexQuery", &RegexQuery{Field: "title", Pattern: "colou?r"}, QueryTypeRegex},
		{"PhraseQuery", &PhraseQuery{Field: "body", Terms: []string{"quick", "fox"}}, QueryTypePhrase},
		{"ProximityQuery", &ProximityQuery{Field: "body", Terms: []string{"quick", "fox"}, Slop: 3}, QueryTypeProximity},
		{"FuzzyQuery", &FuzzyQuery{Field: "title", Term: "search", MaxDistance: 1}, QueryTypeFuzzy},
		{"MatchAllQuery", &MatchAllQuery{}, QueryTypeMatchAll},
		{"MatchNoneQuery", &MatchNoneQuery{}, QueryTypeMatchNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.Type(); got != tt.want {
				t.Errorf("Type() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRewrite_FlattenAND(t *testing.T) {
	// AND(AND(a, b), c) → AND(a, b, c)
	inner := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "a"}},
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "b"}},
		},
	}
	outer := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: inner},
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "c"}},
		},
	}

	result := Rewrite(outer)
	bq, ok := result.(*BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery, got %T", result)
	}
	if len(bq.Clauses) != 3 {
		t.Errorf("expected 3 clauses, got %d", len(bq.Clauses))
	}
}

func TestRewrite_FlattenOR(t *testing.T) {
	// OR(OR(a, b), c) → OR(a, b, c)
	inner := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanShould, Query: &TermQuery{Field: "f", Term: "a"}},
			{Occur: BooleanShould, Query: &TermQuery{Field: "f", Term: "b"}},
		},
	}
	outer := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanShould, Query: inner},
			{Occur: BooleanShould, Query: &TermQuery{Field: "f", Term: "c"}},
		},
	}

	result := Rewrite(outer)
	bq, ok := result.(*BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery, got %T", result)
	}
	if len(bq.Clauses) != 3 {
		t.Errorf("expected 3 clauses, got %d", len(bq.Clauses))
	}
}

func TestRewrite_RemoveMatchAllFromAND(t *testing.T) {
	// AND(a, MatchAll) → a
	q := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "a"}},
			{Occur: BooleanMust, Query: &MatchAllQuery{}},
		},
	}

	result := Rewrite(q)
	if _, ok := result.(*TermQuery); !ok {
		t.Errorf("expected TermQuery, got %T", result)
	}
}

func TestRewrite_ShortCircuitMatchNone(t *testing.T) {
	// AND(a, MatchNone) → MatchNone
	q := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "a"}},
			{Occur: BooleanMust, Query: &MatchNoneQuery{}},
		},
	}

	result := Rewrite(q)
	if _, ok := result.(*MatchNoneQuery); !ok {
		t.Errorf("expected MatchNoneQuery, got %T", result)
	}
}

func TestRewrite_AllMatchAll(t *testing.T) {
	// AND(MatchAll, MatchAll) → MatchAll
	q := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: &MatchAllQuery{}},
			{Occur: BooleanMust, Query: &MatchAllQuery{}},
		},
	}

	result := Rewrite(q)
	if _, ok := result.(*MatchAllQuery); !ok {
		t.Errorf("expected MatchAllQuery, got %T", result)
	}
}

func TestRewrite_LeafQuery(t *testing.T) {
	// Leaf queries pass through unchanged.
	q := &TermQuery{Field: "f", Term: "hello"}
	result := Rewrite(q)
	if result != q {
		t.Error("leaf query should pass through unchanged")
	}
}

func TestRewrite_NoFlattenMustNot(t *testing.T) {
	// NOT(AND(a, b)) should NOT be flattened.
	inner := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "a"}},
			{Occur: BooleanMust, Query: &TermQuery{Field: "f", Term: "b"}},
		},
	}
	outer := &BooleanQuery{
		Clauses: []BooleanClause{
			{Occur: BooleanMustNot, Query: inner},
		},
	}

	result := Rewrite(outer)
	bq, ok := result.(*BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery, got %T", result)
	}
	if len(bq.Clauses) != 1 {
		t.Errorf("expected 1 clause (not flattened), got %d", len(bq.Clauses))
	}
}
