package testutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"GoSearch/internal/analysis"
	"GoSearch/internal/index"
	"GoSearch/internal/indexing"
)

// WithTempDir creates a temporary directory, calls fn with its path,
// and cleans up afterwards.
func WithTempDir(t *testing.T, fn func(dir string)) {
	t.Helper()
	dir := t.TempDir()
	fn(dir)
}

// BasicSchema returns a schema suitable for most tests.
func BasicSchema() *index.Schema {
	return &index.Schema{
		Version:         1,
		CreatedAt:       time.Now(),
		DefaultAnalyzer: "standard",
		Fields: []index.FieldDef{
			{Name: "id", Type: index.FieldTypeKeyword, Stored: true, Indexed: true},
			{Name: "title", Type: index.FieldTypeText, Analyzer: "standard", Stored: true, Indexed: true, Positions: true},
			{Name: "body", Type: index.FieldTypeText, Analyzer: "standard", Stored: false, Indexed: true, Positions: true},
			{Name: "tags", Type: index.FieldTypeKeyword, Stored: true, Indexed: true, MultiValued: true},
			{Name: "metadata", Type: index.FieldTypeStoredOnly, Stored: true, Indexed: false},
		},
	}
}

// MultiFieldSchema returns a schema with many fields for stress testing.
func MultiFieldSchema() *index.Schema {
	s := &index.Schema{
		Version:         1,
		CreatedAt:       time.Now(),
		DefaultAnalyzer: "standard",
	}
	for i := 0; i < 50; i++ {
		s.Fields = append(s.Fields, index.FieldDef{
			Name:     "field_" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Type:     index.FieldTypeText,
			Analyzer: "standard",
			Stored:   i%3 == 0,
			Indexed:  true,
		})
	}
	return s
}

// CreateTestIndexDir creates a fully initialized index directory with schema.
func CreateTestIndexDir(t *testing.T, dir string) *index.IndexDir {
	t.Helper()
	idxDir := index.NewIndexDir(filepath.Join(dir, "test_index"))
	if err := idxDir.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories: %v", err)
	}
	schema := BasicSchema()
	if err := index.WriteSchema(idxDir, schema); err != nil {
		t.Fatalf("WriteSchema: %v", err)
	}
	return idxDir
}

// SampleDocuments returns a small set of test documents.
func SampleDocuments() []indexing.Document {
	return []indexing.Document{
		{Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "Introduction to Search Engines",
			"body":  "Full-text search is a technique for searching documents",
			"tags":  []interface{}{"search", "tutorial"},
		}},
		{Fields: map[string]interface{}{
			"id":    "doc-2",
			"title": "Advanced Query Processing",
			"body":  "Boolean queries combine multiple search terms using AND OR operators",
			"tags":  []interface{}{"search", "advanced"},
		}},
		{Fields: map[string]interface{}{
			"id":    "doc-3",
			"title": "Building an Inverted Index",
			"body":  "An inverted index maps terms to the documents containing them",
			"tags":  []interface{}{"indexing", "tutorial"},
		}},
		{Fields: map[string]interface{}{
			"id":    "doc-4",
			"title": "BM25 Scoring Algorithm",
			"body":  "BM25 is a ranking function used by search engines to estimate relevance",
			"tags":  []interface{}{"scoring", "algorithm"},
		}},
		{Fields: map[string]interface{}{
			"id":    "doc-5",
			"title": "Fuzzy Search with Levenshtein Automata",
			"body":  "Fuzzy search finds terms within an edit distance of the query term",
			"tags":  []interface{}{"search", "fuzzy"},
		}},
	}
}

// IngestDocuments indexes a set of documents into a writer.
func IngestDocuments(t *testing.T, w *indexing.Writer, docs []indexing.Document) {
	t.Helper()
	for _, doc := range docs {
		if err := w.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument(%v): %v", doc.Fields["id"], err)
		}
	}
}

// CreatePopulatedWriter creates a writer with sample documents already ingested.
func CreatePopulatedWriter(t *testing.T) *indexing.Writer {
	t.Helper()
	schema := BasicSchema()
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)
	IngestDocuments(t, w, SampleDocuments())
	return w
}

// AssertFileExists checks that a file exists at the given path.
func AssertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file to exist: %s", path)
	}
}

// AssertDirExists checks that a directory exists at the given path.
func AssertDirExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Errorf("expected directory to exist: %s", path)
		return
	}
	if !info.IsDir() {
		t.Errorf("expected %s to be a directory", path)
	}
}
