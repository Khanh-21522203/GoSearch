package query

// QueryType identifies the kind of query node.
type QueryType int

const (
	QueryTypeTerm QueryType = iota
	QueryTypeBoolean
	QueryTypePrefix
	QueryTypeWildcard
	QueryTypeRegex
	QueryTypePhrase
	QueryTypeProximity
	QueryTypeFuzzy
	QueryTypeMatchAll
	QueryTypeMatchNone
)

// Query is the interface for all query AST nodes.
type Query interface {
	Type() QueryType
}

// Boolean operator limits.
const (
	MaxBooleanClauses = 1024
	MaxBooleanDepth   = 10
)

// Phrase/proximity limits.
const (
	MaxPhraseLength   = 50
	MaxProximityTerms = 10
	MaxProximitySlop  = 100
)

// Fuzzy limits.
const (
	MaxFuzzyDistance    = 2
	MinFuzzyTermLength = 3
	MaxFuzzyExpansion  = 500
)

// Expansion limits.
const (
	MaxTermsExpanded  = 1000
	MaxStatesVisited  = 10000
)
