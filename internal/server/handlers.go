package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"GoSearch/internal/engine"
	"GoSearch/internal/index"
	"GoSearch/internal/indexing"
	"GoSearch/internal/scoring"
)

// Handler holds HTTP handlers for the GoSearch API.
type Handler struct {
	mgr    *IndexManager
	logger *slog.Logger
}

// NewHandler creates a new Handler backed by the given IndexManager.
func NewHandler(mgr *IndexManager, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{mgr: mgr, logger: logger}
}

// RegisterRoutes registers all API routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Index lifecycle.
	mux.HandleFunc("GET /indexes", h.handleListIndexes)
	mux.HandleFunc("POST /indexes", h.handleCreateIndex)
	mux.HandleFunc("GET /indexes/{name}", h.handleGetIndex)
	mux.HandleFunc("DELETE /indexes/{name}", h.handleDeleteIndex)

	// Document ingestion and deletion.
	mux.HandleFunc("POST /indexes/{name}/documents", h.handleIngestDocuments)
	mux.HandleFunc("DELETE /indexes/{name}/documents", h.handleDeleteDocument)

	// Commit.
	mux.HandleFunc("POST /indexes/{name}/commit", h.handleCommit)

	// Search.
	mux.HandleFunc("POST /indexes/{name}/search", h.handleSearch)
}

// --- Index Lifecycle ---

func (h *Handler) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	names := h.mgr.ListIndexes()

	infos := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		inst, err := h.mgr.GetIndex(name)
		if err != nil {
			continue
		}
		infos = append(infos, inst.IndexInfo())
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"indexes": infos,
	})
}

func (h *Handler) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name            string          `json:"name"`
		DefaultAnalyzer string          `json:"default_analyzer"`
		Fields          []index.FieldDef `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "index name is required")
		return
	}

	schema := &index.Schema{
		DefaultAnalyzer: req.DefaultAnalyzer,
		Fields:          req.Fields,
	}

	if err := h.mgr.CreateIndex(req.Name, schema); err != nil {
		if errors.Is(err, ErrIndexExists) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status": "created",
		"name":   req.Name,
	})
}

func (h *Handler) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	inst, err := h.mgr.GetIndex(name)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, inst.IndexInfo())
}

func (h *Handler) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.mgr.DeleteIndex(name); err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
		"name":   name,
	})
}

// --- Document Ingestion ---

func (h *Handler) handleIngestDocuments(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	inst, err := h.mgr.GetIndex(name)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req struct {
		Documents []map[string]interface{} `json:"documents"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Documents) == 0 {
		writeError(w, http.StatusBadRequest, "no documents provided")
		return
	}

	// Acquire writer if not already held.
	writer, err := inst.AcquireWriter()
	if err != nil {
		if errors.Is(err, ErrWriterBusy) {
			writeError(w, http.StatusServiceUnavailable, "writer is busy, retry later")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = writer // Writer is now held by the instance.

	// Convert to indexing.Document.
	docs := make([]indexing.Document, len(req.Documents))
	for i, d := range req.Documents {
		docs[i] = indexing.Document{Fields: d}
	}

	if err := inst.IngestDocuments(docs); err != nil {
		inst.ReleaseWriter()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":             "accepted",
		"documents_received": len(docs),
		"errors":             []string{},
	})
}

// --- Document Deletion ---

func (h *Handler) handleDeleteDocument(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	inst, err := h.mgr.GetIndex(name)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "document id is required")
		return
	}

	// Ensure writer is active.
	inst.writerMu.Lock()
	w2 := inst.writer
	inst.writerMu.Unlock()

	if w2 == nil {
		// Acquire writer if not held.
		w2, err = inst.AcquireWriter()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "writer is busy, retry later")
			return
		}
	}

	if err := w2.DeleteDocument(req.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
		"id":     req.ID,
	})
}

// --- Commit ---

func (h *Handler) handleCommit(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	inst, err := h.mgr.GetIndex(name)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	result, err := inst.Commit(ctx)
	if err != nil {
		if errors.Is(err, ErrIndexEmpty) {
			writeError(w, http.StatusBadRequest, "no documents to commit")
			return
		}
		if errors.Is(err, ErrWriterBusy) {
			writeError(w, http.StatusServiceUnavailable, "no active writer, ingest documents first")
			return
		}
		writeError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
		return
	}

	// Release writer after successful commit.
	inst.ReleaseWriter()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":            "committed",
		"generation":        result.Generation,
		"segment_id":        result.SegmentID,
		"duration_ms":       result.Duration.Milliseconds(),
	})
}

// --- Search ---

// searchRequest represents a search query.
type searchRequest struct {
	Query struct {
		Type  string `json:"type"`
		Field string `json:"field"`
		Value string `json:"value"`
	} `json:"query"`
	TopK    int  `json:"top_k"`
	Explain bool `json:"explain"`
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	inst, err := h.mgr.GetIndex(name)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.TopK <= 0 {
		req.TopK = 10
	}

	start := time.Now()

	// Acquire snapshot for consistent read.
	snap, err := inst.Snapshots.Acquire()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to acquire snapshot: "+err.Error())
		return
	}
	defer func() { _ = snap.Release() }()

	// Create execution context with timeout.
	execCtx := engine.NewExecutionContext(30*time.Second, 10000, 1000)

	// Execute search against the write buffer (MVP: in-memory search).
	// In a full implementation, this would search committed segments via FST + postings.
	hits := executeSearch(inst, req, execCtx)

	took := time.Since(start)

	response := map[string]interface{}{
		"status":     "success",
		"took_ms":    took.Milliseconds(),
		"total_hits": len(hits),
		"generation": snap.Generation,
		"timed_out":  execCtx.TimedOut,
		"hits":       hits,
	}

	writeJSON(w, http.StatusOK, response)
}

// executeSearch performs a search against the index.
// MVP implementation: searches the in-memory inverted index from the write buffer.
func executeSearch(inst *IndexInstance, req searchRequest, execCtx *engine.ExecutionContext) []map[string]interface{} {
	field := req.Query.Field
	value := req.Query.Value

	if field == "" || value == "" {
		return nil
	}

	// Check execution limits.
	if err := execCtx.CheckLimits(); err != nil {
		return nil
	}

	// For MVP, search the committed manifest's segment data is not yet implemented.
	// Instead, search the current write buffer if a writer is active.
	inst.writerMu.Lock()
	w := inst.writer
	inst.writerMu.Unlock()

	if w == nil {
		return nil
	}

	buf := w.Buffer()
	fieldMap, ok := buf.InvertedIndex[field]
	if !ok {
		return nil
	}

	// Handle different query types.
	var matchingTerms []string
	switch req.Query.Type {
	case "term":
		if _, ok := fieldMap[value]; ok {
			matchingTerms = []string{value}
		}
	case "prefix":
		for term := range fieldMap {
			if strings.HasPrefix(term, value) {
				matchingTerms = append(matchingTerms, term)
				execCtx.TermsMatched++
				if err := execCtx.CheckLimits(); err != nil {
					break
				}
			}
		}
	default:
		// Default to term query.
		if _, ok := fieldMap[value]; ok {
			matchingTerms = []string{value}
		}
	}

	if len(matchingTerms) == 0 {
		return nil
	}

	// Collect matching documents using iterators and scorer.
	scorer := scoring.NewBM25Scorer(int64(buf.DocCount), float32(buf.TermCount)/float32(max(buf.DocCount, 1)))
	collector := engine.NewTopKCollector(req.TopK)

	for _, term := range matchingTerms {
		pl := fieldMap[term]
		if pl == nil {
			continue
		}

		idf := scorer.IDF(int64(len(pl.Entries)))

		// Build postings iterator.
		docIDs := make([]uint32, len(pl.Entries))
		freqs := make([]uint32, len(pl.Entries))
		for i, e := range pl.Entries {
			docIDs[i] = e.DocID
			freqs[i] = e.Freq
		}

		it := engine.NewSlicePostingsIterator(docIDs, freqs)
		for it.Next() {
			score := scorer.Score(it.Freq(), 100, idf) // Approximate doc length.
			collector.Collect(it.DocID(), score)
		}
	}

	// Format results.
	results := collector.Results()
	hits := make([]map[string]interface{}, len(results))

	// Build reverse mapping: internal ID → external ID.
	internalToExternal := make(map[uint32]string, len(buf.ExternalToInternal))
	for ext, internal := range buf.ExternalToInternal {
		internalToExternal[internal] = ext
	}

	for i, doc := range results {
		hit := map[string]interface{}{
			"doc_id": doc.DocID,
			"score":  doc.Score,
		}
		if extID, ok := internalToExternal[doc.DocID]; ok {
			hit["id"] = extID
		}

		// Include stored fields if available.
		if stored, ok := buf.StoredFields[doc.DocID]; ok {
			fields := make(map[string]string, len(stored))
			for k, v := range stored {
				fields[k] = string(v)
			}
			hit["stored_fields"] = fields
		}

		if req.Explain {
			for _, term := range matchingTerms {
				pl := fieldMap[term]
				if pl == nil {
					continue
				}
				// Find the term frequency for this doc.
				var tf uint32
				for _, e := range pl.Entries {
					if e.DocID == doc.DocID {
						tf = e.Freq
						break
					}
				}
				explanation := scorer.Explain(field, term, tf, 100, int64(len(pl.Entries)))
				hit["explanation"] = explanation
				break
			}
		}

		hits[i] = hit
	}

	return hits
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]string{
			"message": message,
		},
	})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
