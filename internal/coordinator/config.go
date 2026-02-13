package coordinator

import "time"

// ShardEndpoint describes a shard node's connection details.
type ShardEndpoint struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
}

// Config configures the Coordinator.
type Config struct {
	// Shards is the list of shard endpoints.
	Shards []ShardEndpoint `json:"shards"`

	// QueryTimeout is the maximum time for a full query (fan-out + merge).
	QueryTimeout time.Duration `json:"query_timeout"`

	// PerShardTimeout is the maximum time to wait for a single shard response.
	PerShardTimeout time.Duration `json:"per_shard_timeout"`

	// ConnectTimeout is the maximum time to establish a connection to a shard.
	ConnectTimeout time.Duration `json:"connect_timeout"`

	// MaxRetries is the maximum number of retries per shard on transient failure.
	MaxRetries int `json:"max_retries"`

	// HealthCheckInterval is how often to poll shard health.
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		QueryTimeout:        10 * time.Second,
		PerShardTimeout:     5 * time.Second,
		ConnectTimeout:      2 * time.Second,
		MaxRetries:          1,
		HealthCheckInterval: 10 * time.Second,
	}
}
