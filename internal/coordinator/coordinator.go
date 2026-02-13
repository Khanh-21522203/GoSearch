package coordinator

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrNoShards       = errors.New("coordinator: no shards configured")
	ErrAllShardsFailed = errors.New("coordinator: all shards failed")
)

// ShardClient is the interface that shard nodes must implement.
// The Coordinator uses this to fan out query plans and check health.
type ShardClient interface {
	// Execute sends a query plan to a shard and returns its response.
	Execute(ctx context.Context, plan *QueryPlan) (*ShardResponse, error)

	// Health checks the health of a shard.
	Health(ctx context.Context) (*ShardHealth, error)
}

// Coordinator is a stateless routing and aggregation layer.
// It fans out queries to shard nodes and merges results.
//
// Critical constraint: The Coordinator performs NO query execution.
// All automata construction, FST traversal, postings retrieval, and scoring
// occur exclusively on shard nodes.
type Coordinator struct {
	config  Config
	clients map[string]ShardClient // shardID → client
	logger  *slog.Logger

	healthMu sync.RWMutex
	health   map[string]*ShardHealth // shardID → last known health
}

// New creates a new Coordinator with the given config and shard clients.
func New(config Config, clients map[string]ShardClient, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		config:  config,
		clients: clients,
		logger:  logger,
		health:  make(map[string]*ShardHealth),
	}
}

// QueryResult is the merged result returned to the client.
type QueryResult struct {
	Status           string          `json:"status"` // "success", "partial", "error"
	Hits             []ShardHit      `json:"hits"`
	TotalHits        uint64          `json:"total_hits"`
	TookMs           int64           `json:"took_ms"`
	SuccessfulShards []string        `json:"successful_shards"`
	Errors           []ShardError    `json:"errors,omitempty"`
}

// ShardError describes an error from a specific shard.
type ShardError struct {
	ShardID string `json:"shard_id"`
	Error   string `json:"error"`
}

// Search executes a query across all shards and merges results.
// This implements the 7-step coordinator query flow from the spec.
func (c *Coordinator) Search(ctx context.Context, query QueryClause, opts QueryOptions) (*QueryResult, error) {
	start := time.Now()

	if len(c.clients) == 0 {
		return nil, ErrNoShards
	}

	// Step 1: RECEIVE & PARSE — already done by caller.
	// Step 2: REWRITE — build canonical QueryPlan.
	plan := c.buildQueryPlan(query, opts)

	// Step 3: SNAPSHOT SELECTION — each shard uses its own latest generation.
	// (MVP: no cross-shard generation coordination.)

	// Step 4: FAN-OUT — send to all shards in parallel.
	queryCtx, cancel := context.WithTimeout(ctx, c.config.PerShardTimeout)
	defer cancel()

	responses := c.fanOut(queryCtx, plan)

	// Step 5: COLLECT — gather responses.
	var successful []ShardResponse
	var shardErrors []ShardError
	var successfulShardIDs []string

	for _, resp := range responses {
		if resp.err != nil {
			shardErrors = append(shardErrors, ShardError{
				ShardID: resp.shardID,
				Error:   resp.err.Error(),
			})
			c.logger.Warn("shard query failed",
				"shard", resp.shardID,
				"error", resp.err,
			)
			continue
		}
		if resp.response.Status == "error" {
			shardErrors = append(shardErrors, ShardError{
				ShardID: resp.shardID,
				Error:   resp.response.Error,
			})
			continue
		}
		successful = append(successful, *resp.response)
		successfulShardIDs = append(successfulShardIDs, resp.shardID)
	}

	// All shards failed.
	if len(successful) == 0 {
		return &QueryResult{
			Status:  "error",
			Errors:  shardErrors,
			TookMs:  time.Since(start).Milliseconds(),
		}, ErrAllShardsFailed
	}

	// Step 6: MERGE — merge shard top-K into global top-K.
	merged := mergeTopK(successful, opts.TopK)

	// Aggregate total hits.
	var totalHits uint64
	for _, resp := range successful {
		totalHits += resp.Stats.TotalHits
	}

	// Step 7: RESPOND.
	status := "success"
	if len(shardErrors) > 0 {
		status = "partial"
	}

	return &QueryResult{
		Status:           status,
		Hits:             merged,
		TotalHits:        totalHits,
		TookMs:           time.Since(start).Milliseconds(),
		SuccessfulShards: successfulShardIDs,
		Errors:           shardErrors,
	}, nil
}

// shardResult is an internal type for collecting fan-out results.
type shardResult struct {
	shardID  string
	response *ShardResponse
	err      error
}

// fanOut sends the query plan to all shards in parallel.
func (c *Coordinator) fanOut(ctx context.Context, plan *QueryPlan) []shardResult {
	results := make([]shardResult, 0, len(c.clients))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardID, client := range c.clients {
		wg.Add(1)
		go func(id string, cl ShardClient) {
			defer wg.Done()
			resp, err := cl.Execute(ctx, plan)
			mu.Lock()
			results = append(results, shardResult{
				shardID:  id,
				response: resp,
				err:      err,
			})
			mu.Unlock()
		}(shardID, client)
	}

	wg.Wait()
	return results
}

// buildQueryPlan creates a canonical QueryPlan from the query and options.
func (c *Coordinator) buildQueryPlan(query QueryClause, opts QueryOptions) *QueryPlan {
	return &QueryPlan{
		PlanID:    generatePlanID(),
		TimeoutMs: c.config.PerShardTimeout.Milliseconds(),
		Query:     query,
		Options:   opts,
	}
}

// CheckHealth polls all shards for their health status.
func (c *Coordinator) CheckHealth(ctx context.Context) map[string]*ShardHealth {
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make(map[string]*ShardHealth, len(c.clients))

	for shardID, client := range c.clients {
		wg.Add(1)
		go func(id string, cl ShardClient) {
			defer wg.Done()
			h, err := cl.Health(ctx)
			if err != nil {
				h = &ShardHealth{Status: "unhealthy"}
				c.logger.Warn("shard health check failed", "shard", id, "error", err)
			}
			mu.Lock()
			results[id] = h
			mu.Unlock()
		}(shardID, client)
	}

	wg.Wait()

	// Update cached health.
	c.healthMu.Lock()
	for id, h := range results {
		c.health[id] = h
	}
	c.healthMu.Unlock()

	return results
}

// HealthyShardCount returns the number of shards last known to be healthy.
func (c *Coordinator) HealthyShardCount() int {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()

	count := 0
	for _, h := range c.health {
		if h.Status == "healthy" {
			count++
		}
	}
	return count
}

// mergeTopK merges shard-local top-K results into a global top-K.
// Uses a min-heap of size K.
func mergeTopK(responses []ShardResponse, k int) []ShardHit {
	if k <= 0 {
		k = 10 // Default.
	}

	h := &hitHeap{}
	heap.Init(h)

	for _, resp := range responses {
		for _, hit := range resp.Hits {
			if h.Len() < k {
				heap.Push(h, hit)
			} else if hit.Score > (*h)[0].Score {
				(*h)[0] = hit
				heap.Fix(h, 0)
			}
		}
	}

	// Extract sorted descending by score.
	result := make([]ShardHit, h.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(ShardHit)
	}
	return result
}

// hitHeap is a min-heap of ShardHit ordered by score.
type hitHeap []ShardHit

func (h hitHeap) Len() int            { return len(h) }
func (h hitHeap) Less(i, j int) bool   { return h[i].Score < h[j].Score }
func (h hitHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *hitHeap) Push(x any)          { *h = append(*h, x.(ShardHit)) }
func (h *hitHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func generatePlanID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("plan-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
