# GoSearch

A high-performance, pure-Go full-text search engine built from scratch. GoSearch uses **FST-based term dictionaries**, **automaton-driven query expansion**, **commit-based visibility**, and **snapshot isolation** to deliver fast, consistent, and crash-safe search.

**Zero CGO. Zero mmap. Zero external dependencies.**

---

## Features

### Core Search
- **Full-text indexing** with configurable analyzers (standard, whitespace, keyword)
- **BM25 scoring** with tunable parameters (k1, b) and score explanation API
- **10 query operators**: term, boolean (AND/OR/NOT), prefix, wildcard, regex, phrase, proximity, fuzzy, match_all, match_none
- **Automaton-first query expansion** — prefix, wildcard, regex, and fuzzy queries compile to DFAs intersected with the FST

### Storage & Durability
- **Commit-based visibility** — documents become searchable only after explicit commit
- **7-phase commit protocol** with fsync ordering and atomic manifest updates
- **9-step crash recovery** with manifest fallback, orphan cleanup, and checksum verification
- **Immutable segments** — once written, segment files are never modified
- **SHA-256 checksums** on all persisted files; xxhash64 on FST and postings data

### Concurrency
- **Snapshot isolation** — readers always see a consistent committed generation
- **Reference-counted segments** — safe concurrent access with automatic reclamation
- **Single-writer model** — exclusive writer lock per index prevents conflicts

### Query Safety
- **Bounded automaton construction** — DFA state limits (max 10,000) prevent DoS
- **Query timeout enforcement** with amortized time checks
- **Term expansion limits** (max 1,000 terms) on prefix/wildcard/regex/fuzzy queries
- **Boolean clause limits** (max 1,024) and depth limits (max 10)

### Horizontal Scaling
- **Stateless coordinator** for multi-shard query routing and result merging
- **Per-shard snapshot selection** — each shard uses its own committed generation
- **Partial success handling** — returns results even if some shards fail

### Observability
- **Prometheus metrics** for queries, indexing, commits, and scoring
- **Health check endpoints** for Kubernetes liveness/readiness probes
- **Structured JSON logging** with configurable levels

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        HTTP API                               │
│              (Create Index / Ingest / Search)                 │
└────────────────────────┬─────────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────────┐
│                    Index Manager                              │
│         (Index lifecycle, writer locks, routing)              │
└────────┬───────────────┬──────────────────────┬──────────────┘
         │               │                      │
┌────────▼──────┐ ┌──────▼───────┐  ┌───────────▼──────────┐
│  Index        │ │  Index       │  │  Index               │
│  Instance A   │ │  Instance B  │  │  Instance C          │
│               │ │              │  │                      │
│ ┌───────────┐ │ │              │  │                      │
│ │ Snapshot  │ │ │     ...      │  │        ...           │
│ │ Manager   │ │ │              │  │                      │
│ ├───────────┤ │ └──────────────┘  └──────────────────────┘
│ │ Segments  │ │
│ │ [0][1][2] │ │
│ └───────────┘ │
└───────────────┘
```

### Package Layout

```
internal/
├── analysis/       # Text analyzers (standard, whitespace, keyword)
├── automaton/      # DFA implementations (prefix, wildcard, levenshtein)
├── benchmark/      # Performance benchmarks
├── commit/         # 7-phase commit protocol
├── coordinator/    # Multi-shard query routing and merging
├── engine/         # Query execution (conjunction, disjunction, collector)
├── index/          # Schema, manifest, segment metadata, directory layout
├── indexing/       # Document ingestion, write buffer, writer model
├── integration/    # Integration tests (crash recovery, concurrency, E2E)
├── query/          # Query AST types and limits
├── recovery/       # 9-step crash recovery protocol
├── scoring/        # BM25 scorer with explain API
├── snapshot/       # Snapshot lifecycle and reference counting
├── storage/        # Checksums, fsync, file utilities
└── testutil/       # Test helpers (temp dirs, sample docs, assertions)
```

### On-Disk Layout

```
data/
└── indexes/
    └── my_index/
        ├── schema.json              # Immutable schema definition
        ├── manifests/
        │   ├── manifest.current     # Current generation pointer
        │   └── manifest_gen_1.json  # Generation manifest with checksums
        ├── segments/
        │   └── seg_abc123/
        │       ├── meta.json        # Segment metadata and field stats
        │       ├── fst.bin          # FST term dictionary
        │       ├── postings.bin     # Delta-encoded postings lists
        │       ├── positions.bin    # Term position data
        │       ├── stored.bin       # Stored field values
        │       └── deletions.bin    # Deletion bitmap
        └── tmp/                     # Staging area for atomic writes
```

---

## Quick Start

### Prerequisites

- **Go 1.22+**
- **Docker** (optional, for containerized deployment)

### Build from Source

```bash
git clone https://github.com/your-org/GoSearch.git
cd GoSearch
go build -o gotextsearch ./cmd/server
./gotextsearch --config=config.yaml
```

### Run with Docker

```bash
# Development
docker compose up

# Production (with Prometheus + Grafana)
docker compose -f docker-compose.prod.yml up -d
```

### Run with Docker (build manually)

```bash
docker build -t gotextsearch .
docker run -d \
  -p 8080:8080 \
  -v gotextsearch-data:/data \
  -e GOTEXTSEARCH_LOG_LEVEL=info \
  gotextsearch
```

---

## Usage Guide

### Create an Index

```bash
curl -X PUT http://localhost:8080/indexes/articles \
  -H "Content-Type: application/json" \
  -d '{
    "schema": {
      "fields": [
        {"name": "title",  "type": "text",    "analyzer": "standard", "stored": true, "indexed": true, "positions": true},
        {"name": "body",   "type": "text",    "analyzer": "standard", "stored": true, "indexed": true, "positions": true},
        {"name": "tags",   "type": "keyword", "stored": true, "indexed": true, "multi_valued": true},
        {"name": "status", "type": "keyword", "stored": true, "indexed": true}
      ],
      "default_analyzer": "standard"
    }
  }'
```

### Index Documents

```bash
curl -X POST http://localhost:8080/indexes/articles/documents \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {
        "id": "doc-1",
        "title": "Introduction to Full-Text Search",
        "body": "Full-text search engines index documents and allow fast retrieval...",
        "tags": ["search", "tutorial"],
        "status": "published"
      },
      {
        "id": "doc-2",
        "title": "Building Search from Scratch",
        "body": "This guide covers FST construction, postings encoding, and BM25 scoring...",
        "tags": ["search", "engineering"],
        "status": "published"
      }
    ]
  }'
```

### Commit Changes

Documents are **not searchable** until committed:

```bash
curl -X POST http://localhost:8080/indexes/articles/commit
```

### Search

#### Term Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"term": {"field": "status", "value": "published"}},
    "size": 10
  }'
```

#### Boolean Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "bool": {
        "must": [
          {"term": {"field": "status", "value": "published"}}
        ],
        "should": [
          {"prefix": {"field": "title", "prefix": "search"}}
        ],
        "must_not": [
          {"term": {"field": "tags", "value": "draft"}}
        ]
      }
    },
    "size": 10
  }'
```

#### Phrase Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"phrase": {"field": "body", "value": "full-text search", "slop": 0}},
    "size": 10
  }'
```

#### Fuzzy Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"fuzzy": {"field": "title", "value": "serch", "fuzziness": 1}},
    "size": 10
  }'
```

#### Wildcard Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"wildcard": {"field": "title", "pattern": "search*"}},
    "size": 10
  }'
```

#### Regex Query

```bash
curl -X POST http://localhost:8080/indexes/articles/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"regexp": {"field": "title", "pattern": "colou?r"}},
    "size": 10
  }'
```

#### Score Explanation

```bash
curl -X POST http://localhost:8080/indexes/articles/search?explain=true \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"term": {"field": "title", "value": "search"}},
    "size": 5
  }'
```

---

## Schema Reference

### Field Types

| Type | Description | Indexed | Positions | Analyzable |
|------|-------------|---------|-----------|------------|
| `text` | Full-text analyzed content | Yes | Optional | Yes |
| `keyword` | Exact-match values (tags, status) | Yes | No | No |
| `stored_only` | Stored but not searchable | No | No | No |

### Built-in Analyzers

| Analyzer | Tokenization | Normalization | Use Case |
|----------|-------------|---------------|----------|
| `standard` | Unicode word boundaries | Lowercase | General text |
| `whitespace` | Split on whitespace | None | Case-sensitive, pre-tokenized |
| `keyword` | Entire value as one token | None | Exact match fields |

---

## Configuration

Configuration via `config.yaml` or environment variables:

```yaml
server:
  host: 0.0.0.0
  port: 8080
  read_timeout: 30s
  write_timeout: 60s

storage:
  data_dir: /data
  fsync_on_commit: true

indexing:
  buffer_size: 64MB
  max_docs_per_segment: 100000

query:
  default_timeout: 30s
  max_results: 10000
  max_boolean_clauses: 1024
  automaton_limits:
    max_states: 10000
    max_terms: 1000

logging:
  level: info       # debug, info, warn, error
  format: json
  output: stdout

metrics:
  enabled: true
  port: 9090
  path: /metrics
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOTEXTSEARCH_DATA_DIR` | `/data` | Data storage directory |
| `GOTEXTSEARCH_PORT` | `8080` | HTTP server port |
| `GOTEXTSEARCH_LOG_LEVEL` | `info` | Log level |
| `GOTEXTSEARCH_METRICS_ENABLED` | `true` | Enable Prometheus metrics |

---

## Query Limits

| Limit | Default | Description |
|-------|---------|-------------|
| Max boolean clauses | 1,024 | Prevents excessive nesting |
| Max boolean depth | 10 | Prevents deep recursion |
| Max phrase length | 50 terms | Bounds position checks |
| Max proximity terms | 10 | Limits complexity |
| Max proximity slop | 100 | Reasonable distance |
| Max fuzzy distance | 2 | Beyond 2 is exponential |
| Min fuzzy term length | 3 | Short terms expand too much |
| Max terms expanded | 1,000 | Limits automaton-FST intersection |
| Max automaton states | 10,000 | Bounds DFA construction |
| Max wildcard pattern | 256 chars | Prevents DoS |

---

## Testing

```bash
# Run all tests with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem -count=3 ./internal/benchmark/...

# Run fuzz tests (30s each)
go test -fuzz=Fuzz -fuzztime=30s ./internal/automaton/...
go test -fuzz=Fuzz -fuzztime=30s ./internal/analysis/...

# Run integration tests only
go test -race ./internal/integration/...
```

### Test Categories

- **Unit tests** — per-package correctness (schema, automaton, scoring, analysis, etc.)
- **Integration tests** — crash recovery, concurrent readers, end-to-end index-search cycles
- **Fuzz tests** — wildcard/levenshtein/prefix automata, standard/whitespace analyzers
- **Benchmarks** — indexing throughput, automaton construction, query latency, scoring, memory allocation

---

## Deployment

### Development (with hot reload)

```bash
docker compose -f docker-compose.dev.yml up
```

### Production (with monitoring)

```bash
docker compose -f docker-compose.prod.yml up -d
```

This starts:
- **GoSearch** on port `8080`
- **Prometheus** on port `9090`
- **Grafana** on port `3000` (default password: `admin`)

### Health Check

```bash
curl http://localhost:8080/health
```

---

## Design Principles

1. **Commit-based visibility** — no document is searchable until explicitly committed
2. **Snapshot isolation** — every reader sees a consistent point-in-time view
3. **Automata-first** — all non-trivial term expansion uses DFA ∩ FST intersection
4. **Immutable storage** — segments are never modified after creation
5. **No mmap** — all I/O is explicit for predictable memory behavior
6. **No CGO** — pure Go for simple cross-compilation and deployment
7. **Crash safety** — fsync ordering + manifest-based recovery guarantees durability

---

## Documentation

Detailed design documentation is available in the [`docs/`](docs/) directory:

- **Architecture** — [overview](docs/architecture/overview.md), [commit & recovery](docs/architecture/commit-and-recovery.md), [storage layout](docs/architecture/storage-layout.md), [reader model](docs/architecture/reader-model.md), [coordinator](docs/architecture/coordinator.md)
- **Engine** — [schema](docs/engine/schema.md), [indexing](docs/engine/indexing.md), [term dictionary](docs/engine/term-dictionary.md), [automata](docs/engine/automata.md), [query engine](docs/engine/query-engine.md), [query operators](docs/engine/query-operators.md), [scoring](docs/engine/scoring.md)
- **Operations** — [testing strategy](docs/testing/testing-strategy.md), [benchmarks](docs/benchmarking/benchmarks.md), [Docker deployment](docs/deployment/docker.md)
- **Planning** — [requirements](docs/requirements.md), [roadmap](docs/roadmap.md)

---

## License

MIT
