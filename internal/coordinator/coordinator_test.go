package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockShardClient implements ShardClient for testing.
type mockShardClient struct {
	executeFunc func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error)
	healthFunc  func(ctx context.Context) (*ShardHealth, error)
}

func (m *mockShardClient) Execute(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, plan)
	}
	return &ShardResponse{
		PlanID: plan.PlanID,
		Status: "success",
		Stats:  ShardStats{TotalHits: 0},
		Hits:   nil,
	}, nil
}

func (m *mockShardClient) Health(ctx context.Context) (*ShardHealth, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx)
	}
	return &ShardHealth{Status: "healthy", Generation: 1, Segments: 1, DocCount: 100}, nil
}

func newTestCoordinator(clients map[string]ShardClient) *Coordinator {
	cfg := DefaultConfig()
	cfg.PerShardTimeout = 1 * time.Second
	return New(cfg, clients, nil)
}

func TestSearch_NoShards(t *testing.T) {
	c := newTestCoordinator(nil)
	_, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
	if !errors.Is(err, ErrNoShards) {
		t.Errorf("expected ErrNoShards, got: %v", err)
	}
}

func TestSearch_SingleShard_NoResults(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{},
	}
	c := newTestCoordinator(clients)

	result, err := c.Search(context.Background(), QueryClause{Type: "term", Field: "title", Term: "hello"}, QueryOptions{TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "success" {
		t.Errorf("status = %s, want success", result.Status)
	}
	if len(result.Hits) != 0 {
		t.Errorf("hits = %d, want 0", len(result.Hits))
	}
	if len(result.SuccessfulShards) != 1 {
		t.Errorf("successful shards = %d, want 1", len(result.SuccessfulShards))
	}
}

func TestSearch_SingleShard_WithResults(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return &ShardResponse{
					PlanID: plan.PlanID,
					Status: "success",
					Stats:  ShardStats{TotalHits: 3},
					Hits: []ShardHit{
						{DocID: "doc1", Score: 2.5},
						{DocID: "doc2", Score: 1.8},
						{DocID: "doc3", Score: 1.2},
					},
				}, nil
			},
		},
	}
	c := newTestCoordinator(clients)

	result, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalHits != 3 {
		t.Errorf("total hits = %d, want 3", result.TotalHits)
	}
	if len(result.Hits) != 3 {
		t.Errorf("hits = %d, want 3", len(result.Hits))
	}
	// Should be sorted descending by score.
	if result.Hits[0].Score < result.Hits[1].Score {
		t.Error("hits should be sorted descending by score")
	}
}

func TestSearch_MultiShard_MergeTopK(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return &ShardResponse{
					PlanID:  plan.PlanID,
					ShardID: "shard_0",
					Status:  "success",
					Stats:   ShardStats{TotalHits: 100},
					Hits: []ShardHit{
						{DocID: "s0_doc1", Score: 5.0},
						{DocID: "s0_doc2", Score: 3.0},
						{DocID: "s0_doc3", Score: 1.0},
					},
				}, nil
			},
		},
		"shard_1": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return &ShardResponse{
					PlanID:  plan.PlanID,
					ShardID: "shard_1",
					Status:  "success",
					Stats:   ShardStats{TotalHits: 200},
					Hits: []ShardHit{
						{DocID: "s1_doc1", Score: 4.5},
						{DocID: "s1_doc2", Score: 2.5},
						{DocID: "s1_doc3", Score: 0.5},
					},
				}, nil
			},
		},
	}
	c := newTestCoordinator(clients)

	result, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 3})
	if err != nil {
		t.Fatal(err)
	}

	if result.TotalHits != 300 {
		t.Errorf("total hits = %d, want 300", result.TotalHits)
	}
	if len(result.Hits) != 3 {
		t.Fatalf("hits = %d, want 3", len(result.Hits))
	}

	// Top 3 should be: 5.0, 4.5, 3.0
	expectedScores := []float64{5.0, 4.5, 3.0}
	for i, expected := range expectedScores {
		if result.Hits[i].Score != expected {
			t.Errorf("hit[%d].Score = %f, want %f", i, result.Hits[i].Score, expected)
		}
	}
}

func TestSearch_PartialFailure(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return &ShardResponse{
					PlanID: plan.PlanID,
					Status: "success",
					Stats:  ShardStats{TotalHits: 10},
					Hits:   []ShardHit{{DocID: "doc1", Score: 1.0}},
				}, nil
			},
		},
		"shard_1": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return nil, errors.New("connection refused")
			},
		},
	}
	c := newTestCoordinator(clients)

	result, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "partial" {
		t.Errorf("status = %s, want partial", result.Status)
	}
	if len(result.Errors) != 1 {
		t.Errorf("errors = %d, want 1", len(result.Errors))
	}
	if len(result.Hits) != 1 {
		t.Errorf("hits = %d, want 1", len(result.Hits))
	}
}

func TestSearch_AllShardsFail(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return nil, errors.New("timeout")
			},
		},
		"shard_1": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return nil, errors.New("timeout")
			},
		},
	}
	c := newTestCoordinator(clients)

	_, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
	if !errors.Is(err, ErrAllShardsFailed) {
		t.Errorf("expected ErrAllShardsFailed, got: %v", err)
	}
}

func TestSearch_ShardReturnsError(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				return &ShardResponse{
					PlanID: plan.PlanID,
					Status: "error",
					Error:  "index not found",
				}, nil
			},
		},
	}
	c := newTestCoordinator(clients)

	_, err := c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
	if !errors.Is(err, ErrAllShardsFailed) {
		t.Errorf("expected ErrAllShardsFailed, got: %v", err)
	}
}

func TestMergeTopK_Empty(t *testing.T) {
	result := mergeTopK(nil, 10)
	if len(result) != 0 {
		t.Errorf("expected 0 hits, got %d", len(result))
	}
}

func TestMergeTopK_LessThanK(t *testing.T) {
	responses := []ShardResponse{
		{Hits: []ShardHit{{DocID: "a", Score: 1.0}, {DocID: "b", Score: 2.0}}},
	}
	result := mergeTopK(responses, 10)
	if len(result) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result))
	}
	if result[0].Score != 2.0 {
		t.Errorf("first hit score = %f, want 2.0", result[0].Score)
	}
}

func TestMergeTopK_ExactlyK(t *testing.T) {
	responses := []ShardResponse{
		{Hits: []ShardHit{{DocID: "a", Score: 3.0}, {DocID: "b", Score: 1.0}}},
		{Hits: []ShardHit{{DocID: "c", Score: 2.0}}},
	}
	result := mergeTopK(responses, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 hits, got %d", len(result))
	}
	// Sorted descending: 3.0, 2.0, 1.0
	if result[0].Score != 3.0 || result[1].Score != 2.0 || result[2].Score != 1.0 {
		t.Errorf("unexpected order: %v, %v, %v", result[0].Score, result[1].Score, result[2].Score)
	}
}

func TestMergeTopK_MoreThanK(t *testing.T) {
	responses := []ShardResponse{
		{Hits: []ShardHit{
			{DocID: "a", Score: 5.0},
			{DocID: "b", Score: 3.0},
			{DocID: "c", Score: 1.0},
		}},
		{Hits: []ShardHit{
			{DocID: "d", Score: 4.0},
			{DocID: "e", Score: 2.0},
		}},
	}
	result := mergeTopK(responses, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 hits, got %d", len(result))
	}
	// Top 3: 5.0, 4.0, 3.0
	if result[0].Score != 5.0 || result[1].Score != 4.0 || result[2].Score != 3.0 {
		t.Errorf("unexpected scores: %v, %v, %v", result[0].Score, result[1].Score, result[2].Score)
	}
}

func TestMergeTopK_DefaultK(t *testing.T) {
	responses := []ShardResponse{
		{Hits: []ShardHit{{DocID: "a", Score: 1.0}}},
	}
	result := mergeTopK(responses, 0)
	if len(result) != 1 {
		t.Errorf("expected 1 hit with default K, got %d", len(result))
	}
}

func TestCheckHealth(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{},
		"shard_1": &mockShardClient{
			healthFunc: func(ctx context.Context) (*ShardHealth, error) {
				return nil, errors.New("unreachable")
			},
		},
	}
	c := newTestCoordinator(clients)

	health := c.CheckHealth(context.Background())
	if len(health) != 2 {
		t.Fatalf("health entries = %d, want 2", len(health))
	}
	if health["shard_0"].Status != "healthy" {
		t.Errorf("shard_0 status = %s, want healthy", health["shard_0"].Status)
	}
	if health["shard_1"].Status != "unhealthy" {
		t.Errorf("shard_1 status = %s, want unhealthy", health["shard_1"].Status)
	}

	if c.HealthyShardCount() != 1 {
		t.Errorf("healthy count = %d, want 1", c.HealthyShardCount())
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.QueryTimeout != 10*time.Second {
		t.Errorf("QueryTimeout = %v, want 10s", cfg.QueryTimeout)
	}
	if cfg.PerShardTimeout != 5*time.Second {
		t.Errorf("PerShardTimeout = %v, want 5s", cfg.PerShardTimeout)
	}
	if cfg.MaxRetries != 1 {
		t.Errorf("MaxRetries = %d, want 1", cfg.MaxRetries)
	}
}

func TestQueryPlan_HasPlanID(t *testing.T) {
	clients := map[string]ShardClient{
		"shard_0": &mockShardClient{
			executeFunc: func(ctx context.Context, plan *QueryPlan) (*ShardResponse, error) {
				if plan.PlanID == "" {
					t.Error("plan ID should not be empty")
				}
				if plan.TimeoutMs <= 0 {
					t.Error("timeout should be positive")
				}
				return &ShardResponse{PlanID: plan.PlanID, Status: "success"}, nil
			},
		},
	}
	c := newTestCoordinator(clients)
	_, _ = c.Search(context.Background(), QueryClause{Type: "term"}, QueryOptions{TopK: 10})
}
