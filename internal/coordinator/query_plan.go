package coordinator

// QueryClause represents a single clause in a query plan.
type QueryClause struct {
	Type     string        `json:"type"`
	Field    string        `json:"field,omitempty"`
	Term     string        `json:"term,omitempty"`
	Prefix   string        `json:"prefix,omitempty"`
	Pattern  string        `json:"pattern,omitempty"`
	Operator string        `json:"operator,omitempty"`
	Clauses  []QueryClause `json:"clauses,omitempty"`
}

// QueryOptions specifies result formatting options.
type QueryOptions struct {
	TopK          int      `json:"top_k"`
	Offset        int      `json:"offset"`
	IncludeScores bool     `json:"include_scores"`
	IncludeStored []string `json:"include_stored,omitempty"`
}

// QueryPlan is the canonical query representation sent to shard nodes.
// The Coordinator constructs this from the client request and fans it out.
// NO automaton construction occurs in the QueryPlan — that is shard-local.
type QueryPlan struct {
	PlanID     string      `json:"plan_id"`
	Generation uint64      `json:"generation,omitempty"`
	TimeoutMs  int64       `json:"timeout_ms"`
	Query      QueryClause `json:"query"`
	Options    QueryOptions `json:"options"`
}

// ShardHit represents a single search result from a shard.
type ShardHit struct {
	DocID      string            `json:"doc_id"`
	LocalDocID uint64            `json:"local_doc_id"`
	Score      float64           `json:"score"`
	Stored     map[string]string `json:"stored,omitempty"`
}

// ShardStats contains execution statistics from a shard.
type ShardStats struct {
	TotalHits       uint64 `json:"total_hits"`
	ExecutionTimeMs int64  `json:"execution_time_ms"`
	TermsExpanded   int    `json:"terms_expanded"`
}

// ShardResponse is the response from a single shard node.
type ShardResponse struct {
	PlanID     string     `json:"plan_id"`
	ShardID    string     `json:"shard_id"`
	Generation uint64     `json:"generation"`
	Status     string     `json:"status"` // "success" or "error"
	Error      string     `json:"error,omitempty"`
	Stats      ShardStats `json:"stats"`
	Hits       []ShardHit `json:"hits"`
}

// ShardHealth represents the health status of a shard node.
type ShardHealth struct {
	Status     string `json:"status"` // "healthy", "unhealthy", "unknown"
	Generation uint64 `json:"generation"`
	Segments   int    `json:"segments"`
	DocCount   uint64 `json:"doc_count"`
}
