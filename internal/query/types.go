package query

// TermQuery matches documents containing the exact analyzed term.
type TermQuery struct {
	Field string
	Term  string
	Boost float32
}

func (q *TermQuery) Type() QueryType { return QueryTypeTerm }

// BooleanOp defines the boolean operator.
type BooleanOp int

const (
	BooleanMust    BooleanOp = iota // AND
	BooleanShould                   // OR
	BooleanMustNot                  // NOT
)

// BooleanClause is a single clause within a BooleanQuery.
type BooleanClause struct {
	Occur BooleanOp
	Query Query
}

// BooleanQuery combines sub-queries with boolean logic.
type BooleanQuery struct {
	Clauses            []BooleanClause
	MinimumShouldMatch int
}

func (q *BooleanQuery) Type() QueryType { return QueryTypeBoolean }

// PrefixQuery matches terms starting with the given prefix.
type PrefixQuery struct {
	Field  string
	Prefix string
	Boost  float32
}

func (q *PrefixQuery) Type() QueryType { return QueryTypePrefix }

// WildcardQuery matches terms using wildcard patterns (* and ?).
type WildcardQuery struct {
	Field   string
	Pattern string
	Boost   float32
}

func (q *WildcardQuery) Type() QueryType { return QueryTypeWildcard }

// RegexQuery matches terms matching a regular expression.
type RegexQuery struct {
	Field   string
	Pattern string
	Boost   float32
}

func (q *RegexQuery) Type() QueryType { return QueryTypeRegex }

// PhraseQuery matches documents where terms appear in exact sequence.
type PhraseQuery struct {
	Field string
	Terms []string
	Slop  int
	Boost float32
}

func (q *PhraseQuery) Type() QueryType { return QueryTypePhrase }

// ProximityQuery matches documents where terms appear within a distance.
type ProximityQuery struct {
	Field string
	Terms []string
	Slop  int
	Boost float32
}

func (q *ProximityQuery) Type() QueryType { return QueryTypeProximity }

// FuzzyQuery matches terms within an edit distance of the query term.
type FuzzyQuery struct {
	Field        string
	Term         string
	MaxDistance   int
	PrefixLength int
	Boost        float32
}

func (q *FuzzyQuery) Type() QueryType { return QueryTypeFuzzy }

// MatchAllQuery matches all documents.
type MatchAllQuery struct {
	Boost float32
}

func (q *MatchAllQuery) Type() QueryType { return QueryTypeMatchAll }

// MatchNoneQuery matches no documents.
type MatchNoneQuery struct{}

func (q *MatchNoneQuery) Type() QueryType { return QueryTypeMatchNone }
