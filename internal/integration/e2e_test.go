package integration

import (
	"testing"

	"GoSearch/internal/analysis"
	"GoSearch/internal/engine"
	"GoSearch/internal/index"
	"GoSearch/internal/indexing"
	"GoSearch/internal/scoring"
	"GoSearch/internal/testutil"
)

func TestE2E_IndexSearchCycle(t *testing.T) {
	schema := testutil.BasicSchema()
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)

	// Ingest documents.
	docs := testutil.SampleDocuments()
	testutil.IngestDocuments(t, w, docs)

	buf := w.Buffer()

	// Verify document count.
	if buf.DocCount != len(docs) {
		t.Fatalf("DocCount = %d, want %d", buf.DocCount, len(docs))
	}

	// Simulate term query: search for "search" in title.
	titleIndex := buf.InvertedIndex["title"]
	if titleIndex == nil {
		t.Fatal("title field not indexed")
	}

	pl := titleIndex["search"]
	if pl == nil {
		t.Fatal("term 'search' not found in title index")
	}

	// Build postings iterator from buffer data.
	docIDs := make([]uint32, len(pl.Entries))
	freqs := make([]uint32, len(pl.Entries))
	for i, e := range pl.Entries {
		docIDs[i] = e.DocID
		freqs[i] = e.Freq
	}

	it := engine.NewSlicePostingsIterator(docIDs, freqs)

	// Collect results with scorer.
	scorer := scoring.NewBM25Scorer(int64(buf.DocCount), 10.0)
	idf := scorer.IDF(int64(len(pl.Entries)))

	collector := engine.NewTopKCollector(10)
	for it.Next() {
		score := scorer.Score(it.Freq(), 10, idf)
		collector.Collect(it.DocID(), score)
	}

	results := collector.Results()
	if len(results) == 0 {
		t.Fatal("expected search results")
	}

	// Results should be sorted descending by score.
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted: [%d].Score=%f > [%d].Score=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}
}

func TestE2E_BooleanAND(t *testing.T) {
	schema := testutil.BasicSchema()
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)
	testutil.IngestDocuments(t, w, testutil.SampleDocuments())

	buf := w.Buffer()

	// AND query: "search" AND "engines" in body.
	bodyIndex := buf.InvertedIndex["body"]
	if bodyIndex == nil {
		t.Fatal("body field not indexed")
	}

	searchPL := bodyIndex["search"]
	enginesPL := bodyIndex["engines"]

	if searchPL == nil || enginesPL == nil {
		t.Skip("terms not found in body index")
	}

	// Build iterators.
	searchIt := postingsIteratorFromList(searchPL)
	enginesIt := postingsIteratorFromList(enginesPL)

	conj := engine.NewConjunctionIterator([]engine.PostingsIterator{searchIt, enginesIt})

	var matchedDocs []uint32
	for conj.Next() {
		matchedDocs = append(matchedDocs, conj.DocID())
	}

	// Should find docs containing both "search" and "engines".
	if len(matchedDocs) == 0 {
		t.Error("expected at least one doc matching 'search AND engines'")
	}
}

func TestE2E_BooleanOR(t *testing.T) {
	schema := testutil.BasicSchema()
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)
	testutil.IngestDocuments(t, w, testutil.SampleDocuments())

	buf := w.Buffer()

	tagsIndex := buf.InvertedIndex["tags"]
	if tagsIndex == nil {
		t.Fatal("tags field not indexed")
	}

	searchPL := tagsIndex["search"]
	tutorialPL := tagsIndex["tutorial"]

	if searchPL == nil || tutorialPL == nil {
		t.Skip("terms not found in tags index")
	}

	searchIt := postingsIteratorFromList(searchPL)
	tutorialIt := postingsIteratorFromList(tutorialPL)

	disj := engine.NewDisjunctionIterator([]engine.PostingsIterator{searchIt, tutorialIt})

	var matchedDocs []uint32
	for disj.Next() {
		matchedDocs = append(matchedDocs, disj.DocID())
	}

	// Should find docs with "search" OR "tutorial" in tags.
	if len(matchedDocs) < 2 {
		t.Errorf("expected at least 2 docs, got %d", len(matchedDocs))
	}

	// Docs should be in ascending order.
	for i := 1; i < len(matchedDocs); i++ {
		if matchedDocs[i] <= matchedDocs[i-1] {
			t.Errorf("docs not in order: %d <= %d", matchedDocs[i], matchedDocs[i-1])
		}
	}
}

func TestE2E_MultipleIndexes(t *testing.T) {
	schema := testutil.BasicSchema()
	registry := analysis.NewRegistry()

	// Create two independent writers.
	w1 := indexing.NewWriter(schema, registry)
	w2 := indexing.NewWriter(schema, registry)

	// Index different docs in each.
	w1.AddDocument(indexing.Document{Fields: map[string]interface{}{
		"id": "a1", "title": "Alpha Document",
	}})
	w2.AddDocument(indexing.Document{Fields: map[string]interface{}{
		"id": "b1", "title": "Beta Document",
	}})

	// Verify isolation.
	if w1.Buffer().DocCount != 1 {
		t.Errorf("w1 DocCount = %d, want 1", w1.Buffer().DocCount)
	}
	if w2.Buffer().DocCount != 1 {
		t.Errorf("w2 DocCount = %d, want 1", w2.Buffer().DocCount)
	}

	// w1 should have "alpha", w2 should not.
	if _, ok := w1.Buffer().InvertedIndex["title"]["alpha"]; !ok {
		t.Error("w1 should have 'alpha' in title index")
	}
	if _, ok := w2.Buffer().InvertedIndex["title"]["alpha"]; ok {
		t.Error("w2 should NOT have 'alpha' in title index")
	}
}

func TestE2E_StoredFieldRetrieval(t *testing.T) {
	schema := &index.Schema{
		Version:         1,
		DefaultAnalyzer: "standard",
		Fields: []index.FieldDef{
			{Name: "id", Type: index.FieldTypeKeyword, Stored: true, Indexed: true},
			{Name: "title", Type: index.FieldTypeText, Analyzer: "standard", Stored: true, Indexed: true},
			{Name: "metadata", Type: index.FieldTypeStoredOnly, Stored: true, Indexed: false},
		},
	}
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)

	w.AddDocument(indexing.Document{Fields: map[string]interface{}{
		"id":       "doc-1",
		"title":    "Test Document",
		"metadata": "some raw data",
	}})

	buf := w.Buffer()

	// Stored fields should be retrievable.
	stored := buf.StoredFields[0]
	if stored == nil {
		t.Fatal("no stored fields for doc 0")
	}
	if string(stored["title"]) != "Test Document" {
		t.Errorf("stored title = %q, want %q", stored["title"], "Test Document")
	}
	if string(stored["metadata"]) != "some raw data" {
		t.Errorf("stored metadata = %q, want %q", stored["metadata"], "some raw data")
	}

	// metadata should NOT be indexed.
	if _, ok := buf.InvertedIndex["metadata"]; ok {
		t.Error("stored_only field should not be indexed")
	}
}

func postingsIteratorFromList(pl *indexing.PostingsList) engine.PostingsIterator {
	docIDs := make([]uint32, len(pl.Entries))
	freqs := make([]uint32, len(pl.Entries))
	for i, e := range pl.Entries {
		docIDs[i] = e.DocID
		freqs[i] = e.Freq
	}
	return engine.NewSlicePostingsIterator(docIDs, freqs)
}
