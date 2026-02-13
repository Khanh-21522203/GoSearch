package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"GoSearch/internal/analysis"
	"GoSearch/internal/commit"
	"GoSearch/internal/index"
	"GoSearch/internal/indexing"
	"GoSearch/internal/recovery"
	"GoSearch/internal/snapshot"
)

var (
	ErrIndexNotFound    = errors.New("index not found")
	ErrIndexExists      = errors.New("index already exists")
	ErrWriterBusy       = errors.New("writer is held by another operation")
	ErrIndexEmpty       = errors.New("no documents to commit")
)

// IndexInstance holds all runtime state for a single index.
type IndexInstance struct {
	Name     string
	Dir      *index.IndexDir
	Schema   *index.Schema
	Registry *analysis.Registry

	// Writer state (single-writer model).
	writerMu sync.Mutex
	writer   *indexing.Writer

	// Snapshot manager for reader isolation.
	Snapshots *snapshot.Manager

	// Committer for the 7-phase commit protocol.
	Committer *commit.Committer

	// Current manifest (nil for empty index).
	manifestMu      sync.RWMutex
	currentManifest *index.Manifest

	logger *slog.Logger
}

// IndexManager manages multiple indexes within a single process.
type IndexManager struct {
	rootDir  *index.RootDir
	logger   *slog.Logger
	registry *analysis.Registry

	mu      sync.RWMutex
	indexes map[string]*IndexInstance
}

// NewIndexManager creates a new IndexManager rooted at the given data directory.
func NewIndexManager(dataDir string, logger *slog.Logger) (*IndexManager, error) {
	if logger == nil {
		logger = slog.Default()
	}

	rootDir := index.NewRootDir(dataDir)
	if err := rootDir.EnsureDirectories(); err != nil {
		return nil, fmt.Errorf("ensure root directories: %w", err)
	}

	mgr := &IndexManager{
		rootDir:  rootDir,
		logger:   logger,
		registry: analysis.NewRegistry(),
		indexes:  make(map[string]*IndexInstance),
	}

	// Load existing indexes from disk.
	if err := mgr.loadExistingIndexes(); err != nil {
		return nil, fmt.Errorf("load existing indexes: %w", err)
	}

	return mgr, nil
}

// loadExistingIndexes discovers and opens all indexes on disk.
func (m *IndexManager) loadExistingIndexes() error {
	names, err := m.rootDir.ListIndexes()
	if err != nil {
		return err
	}

	for _, name := range names {
		m.logger.Info("loading index", "name", name)
		inst, err := m.openIndex(name)
		if err != nil {
			m.logger.Error("failed to load index", "name", name, "error", err)
			continue // Skip corrupt indexes, log error.
		}
		m.indexes[name] = inst
		m.logger.Info("index loaded",
			"name", name,
			"generation", inst.Snapshots.CurrentGeneration(),
		)
	}
	return nil
}

// openIndex opens an existing index from disk, running recovery.
func (m *IndexManager) openIndex(name string) (*IndexInstance, error) {
	idxDir := m.rootDir.IndexDir(name)

	// Load schema.
	schema, err := index.LoadSchema(idxDir)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	// Run crash recovery.
	recoveryOpts := recovery.DefaultOptions()
	recoveryOpts.Logger = m.logger.With("index", name, "phase", "recovery")
	result, err := recovery.Recover(idxDir, recoveryOpts)
	if err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}

	// Extract segment IDs from recovered manifest.
	var segmentIDs []string
	if result.Manifest != nil {
		segmentIDs = make([]string, len(result.Manifest.Segments))
		for i, seg := range result.Manifest.Segments {
			segmentIDs[i] = seg.ID
		}
	}

	// Initialize snapshot manager.
	snapLogger := m.logger.With("index", name, "component", "snapshot")
	snapMgr := snapshot.NewManager(result.Generation, segmentIDs, snapLogger)

	// Initialize committer.
	commitOpts := commit.Options{
		SchemaVersion: schema.Version,
		Logger:        m.logger.With("index", name, "component", "commit"),
	}
	committer := commit.NewCommitter(idxDir, commitOpts)

	return &IndexInstance{
		Name:            name,
		Dir:             idxDir,
		Schema:          schema,
		Registry:        m.registry,
		Snapshots:       snapMgr,
		Committer:       committer,
		currentManifest: result.Manifest,
		logger:          m.logger.With("index", name),
	}, nil
}

// CreateIndex creates a new index with the given schema.
func (m *IndexManager) CreateIndex(name string, schema *index.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; exists {
		return ErrIndexExists
	}

	// Validate schema.
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	schema.CreatedAt = time.Now().UTC()
	if schema.Version == 0 {
		schema.Version = 1
	}

	// Create index directory structure.
	idxDir := m.rootDir.IndexDir(name)
	if err := idxDir.EnsureDirectories(); err != nil {
		return fmt.Errorf("create index directories: %w", err)
	}

	// Write schema.
	if err := index.WriteSchema(idxDir, schema); err != nil {
		// Clean up on failure.
		_ = os.RemoveAll(idxDir.Root)
		return fmt.Errorf("write schema: %w", err)
	}

	// Initialize runtime state.
	snapLogger := m.logger.With("index", name, "component", "snapshot")
	snapMgr := snapshot.NewManager(0, nil, snapLogger)

	commitOpts := commit.Options{
		SchemaVersion: schema.Version,
		Logger:        m.logger.With("index", name, "component", "commit"),
	}
	committer := commit.NewCommitter(idxDir, commitOpts)

	inst := &IndexInstance{
		Name:      name,
		Dir:       idxDir,
		Schema:    schema,
		Registry:  m.registry,
		Snapshots: snapMgr,
		Committer: committer,
		logger:    m.logger.With("index", name),
	}

	m.indexes[name] = inst
	m.logger.Info("index created", "name", name)
	return nil
}

// DeleteIndex removes an index and all its data.
func (m *IndexManager) DeleteIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.indexes[name]
	if !exists {
		return ErrIndexNotFound
	}

	// Check for active snapshots.
	if inst.Snapshots.ActiveSnapshotCount() > 0 {
		return fmt.Errorf("cannot delete index with %d active readers", inst.Snapshots.ActiveSnapshotCount())
	}

	// Remove from disk.
	if err := os.RemoveAll(inst.Dir.Root); err != nil {
		return fmt.Errorf("remove index directory: %w", err)
	}

	delete(m.indexes, name)
	m.logger.Info("index deleted", "name", name)
	return nil
}

// GetIndex returns the IndexInstance for the given name.
func (m *IndexManager) GetIndex(name string) (*IndexInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, exists := m.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return inst, nil
}

// ListIndexes returns the names of all loaded indexes.
func (m *IndexManager) ListIndexes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

// AcquireWriter returns an exclusive writer for the index.
// The caller must call ReleaseWriter when done.
func (inst *IndexInstance) AcquireWriter() (*indexing.Writer, error) {
	inst.writerMu.Lock()
	if inst.writer != nil {
		inst.writerMu.Unlock()
		return nil, ErrWriterBusy
	}
	w := indexing.NewWriter(inst.Schema, inst.Registry)
	inst.writer = w
	inst.writerMu.Unlock()
	return w, nil
}

// ReleaseWriter releases the exclusive writer.
func (inst *IndexInstance) ReleaseWriter() {
	inst.writerMu.Lock()
	if inst.writer != nil {
		inst.writer.Release()
		inst.writer = nil
	}
	inst.writerMu.Unlock()
}

// IngestDocuments adds documents to the writer's buffer.
// The writer must be acquired first via AcquireWriter.
func (inst *IndexInstance) IngestDocuments(docs []indexing.Document) error {
	inst.writerMu.Lock()
	w := inst.writer
	inst.writerMu.Unlock()

	if w == nil {
		return ErrWriterBusy
	}

	return w.AddDocuments(docs)
}

// Commit executes the 7-phase commit protocol.
func (inst *IndexInstance) Commit(ctx context.Context) (*commit.CommitResult, error) {
	inst.writerMu.Lock()
	w := inst.writer
	inst.writerMu.Unlock()

	if w == nil {
		return nil, ErrWriterBusy
	}

	buf := w.Buffer()
	if buf.DocCount == 0 {
		return nil, ErrIndexEmpty
	}

	// Build segment data from write buffer.
	segData := buildSegmentData(buf)

	// Get current manifest.
	inst.manifestMu.RLock()
	currentManifest := inst.currentManifest
	inst.manifestMu.RUnlock()

	// Execute commit.
	result, err := inst.Committer.Commit(ctx, currentManifest, segData)
	if err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	// Load new manifest.
	newManifest, err := index.LoadManifest(inst.Dir, result.Generation)
	if err != nil {
		return nil, fmt.Errorf("load new manifest: %w", err)
	}

	// Update snapshot manager.
	segmentIDs := make([]string, len(newManifest.Segments))
	for i, seg := range newManifest.Segments {
		segmentIDs[i] = seg.ID
	}
	reclaimable := inst.Snapshots.UpdateGeneration(result.Generation, segmentIDs)

	// Reclaim old segments.
	for _, segID := range reclaimable {
		segDir := inst.Dir.SegmentDir(segID)
		if err := os.RemoveAll(segDir); err != nil {
			inst.logger.Warn("failed to reclaim segment", "segment", segID, "error", err)
		}
	}

	// Update current manifest.
	inst.manifestMu.Lock()
	inst.currentManifest = newManifest
	inst.manifestMu.Unlock()

	// Reset writer buffer for next batch.
	w.Abort()

	inst.logger.Info("commit complete",
		"generation", result.Generation,
		"segment", result.SegmentID,
		"duration", result.Duration,
	)

	return result, nil
}

// buildSegmentData converts a WriteBuffer into SegmentData for the committer.
func buildSegmentData(buf *indexing.WriteBuffer) *commit.SegmentData {
	// Serialize the inverted index and stored fields into segment files.
	// For MVP, we store the raw data as JSON.
	files := make(map[string][]byte)

	// FST placeholder: serialize term dictionary.
	fstData := serializeTermDictionary(buf)
	files["fst.bin"] = fstData

	// Postings: serialize postings lists.
	postingsData := serializePostings(buf)
	files["postings.bin"] = postingsData

	// Stored fields.
	storedData := serializeStoredFields(buf)
	files["stored.bin"] = storedData

	// Segment metadata.
	metaData := serializeSegmentMeta(buf)
	files["meta.json"] = metaData

	return &commit.SegmentData{
		Files:         files,
		DocCount:      uint32(buf.DocCount),
		DocCountAlive: uint32(buf.DocCount),
		DelCount:      0,
		MinDocID:      0,
		MaxDocID:      uint64(buf.NextDocID),
	}
}

// serializeTermDictionary serializes the inverted index term dictionary.
func serializeTermDictionary(buf *indexing.WriteBuffer) []byte {
	// MVP: collect all unique terms per field.
	type termEntry struct {
		Field string `json:"field"`
		Term  string `json:"term"`
		Count int    `json:"count"`
	}
	var entries []termEntry
	for field, terms := range buf.InvertedIndex {
		for term, pl := range terms {
			entries = append(entries, termEntry{
				Field: field,
				Term:  term,
				Count: len(pl.Entries),
			})
		}
	}
	data, _ := encodeJSON(entries)
	return data
}

// serializePostings serializes postings lists.
func serializePostings(buf *indexing.WriteBuffer) []byte {
	data, _ := encodeJSON(buf.InvertedIndex)
	return data
}

// serializeStoredFields serializes stored field values.
func serializeStoredFields(buf *indexing.WriteBuffer) []byte {
	data, _ := encodeJSON(buf.StoredFields)
	return data
}

// serializeSegmentMeta serializes segment metadata.
func serializeSegmentMeta(buf *indexing.WriteBuffer) []byte {
	meta := map[string]interface{}{
		"doc_count":  buf.DocCount,
		"term_count": buf.TermCount,
	}
	data, _ := encodeJSON(meta)
	return data
}

// IndexInfo returns summary information about an index.
func (inst *IndexInstance) IndexInfo() map[string]interface{} {
	inst.manifestMu.RLock()
	manifest := inst.currentManifest
	inst.manifestMu.RUnlock()

	info := map[string]interface{}{
		"name":             inst.Name,
		"generation":       inst.Snapshots.CurrentGeneration(),
		"active_snapshots": inst.Snapshots.ActiveSnapshotCount(),
		"schema_version":   inst.Schema.Version,
		"fields":           len(inst.Schema.Fields),
	}

	if manifest != nil {
		info["segments"] = len(manifest.Segments)
		info["total_docs"] = manifest.TotalDocs
		info["total_docs_alive"] = manifest.TotalDocsAlive
		info["total_size_bytes"] = manifest.TotalSizeBytes
	} else {
		info["segments"] = 0
		info["total_docs"] = 0
	}

	// Include buffer stats if writer is active.
	inst.writerMu.Lock()
	if inst.writer != nil {
		buf := inst.writer.Buffer()
		info["buffer_docs"] = buf.DocCount
		info["buffer_memory_bytes"] = buf.MemoryUsed()
	}
	inst.writerMu.Unlock()

	return info
}

