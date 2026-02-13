package indexing

import (
	"testing"

	"GoSearch/internal/analysis"
	"GoSearch/internal/index"
)

func testSchema() *index.Schema {
	return &index.Schema{
		Version: 1,
		Fields: []index.FieldDef{
			{Name: "id", Type: index.FieldTypeKeyword, Stored: true, Indexed: true},
			{Name: "title", Type: index.FieldTypeText, Analyzer: "standard", Stored: true, Indexed: true, Positions: true},
			{Name: "body", Type: index.FieldTypeText, Analyzer: "standard", Stored: false, Indexed: true, Positions: true},
			{Name: "tags", Type: index.FieldTypeKeyword, Stored: true, Indexed: true, MultiValued: true},
			{Name: "metadata", Type: index.FieldTypeStoredOnly, Stored: true, Indexed: false},
		},
		DefaultAnalyzer: "standard",
	}
}

func TestWriteBuffer_AllocateDocID(t *testing.T) {
	buf := NewWriteBuffer()

	id1, err := buf.AllocateDocID("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != 0 {
		t.Errorf("first doc ID = %d, want 0", id1)
	}

	id2, err := buf.AllocateDocID("doc-2")
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 1 {
		t.Errorf("second doc ID = %d, want 1", id2)
	}

	if buf.DocCount != 2 {
		t.Errorf("DocCount = %d, want 2", buf.DocCount)
	}
}

func TestWriteBuffer_DuplicateDocID(t *testing.T) {
	buf := NewWriteBuffer()

	_, err := buf.AllocateDocID("doc-1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = buf.AllocateDocID("doc-1")
	if err != ErrDuplicateDoc {
		t.Errorf("expected ErrDuplicateDoc, got %v", err)
	}
}

func TestWriteBuffer_AddPosting(t *testing.T) {
	buf := NewWriteBuffer()

	buf.AddPosting("title", "hello", 0, 2, []uint32{0, 5})
	buf.AddPosting("title", "world", 0, 1, []uint32{1})
	buf.AddPosting("title", "hello", 1, 1, []uint32{0})

	if buf.TermCount != 2 {
		t.Errorf("TermCount = %d, want 2", buf.TermCount)
	}

	pl := buf.InvertedIndex["title"]["hello"]
	if len(pl.Entries) != 2 {
		t.Errorf("hello entries = %d, want 2", len(pl.Entries))
	}
}

func TestWriteBuffer_StoreField(t *testing.T) {
	buf := NewWriteBuffer()

	buf.StoreField(0, "title", []byte("Hello World"))
	buf.StoreField(0, "body", []byte("Some content"))

	fields := buf.StoredFields[0]
	if string(fields["title"]) != "Hello World" {
		t.Errorf("stored title = %q, want %q", fields["title"], "Hello World")
	}
}

func TestWriteBuffer_IsFull_DocLimit(t *testing.T) {
	buf := NewWriteBuffer()
	buf.MaxDocs = 2

	if _, err := buf.AllocateDocID("doc-1"); err != nil {
		t.Fatal(err)
	}
	if buf.IsFull() {
		t.Error("should not be full with 1 doc")
	}

	if _, err := buf.AllocateDocID("doc-2"); err != nil {
		t.Fatal(err)
	}
	if !buf.IsFull() {
		t.Error("should be full with 2 docs")
	}
}

func TestWriteBuffer_Reset(t *testing.T) {
	buf := NewWriteBuffer()
	if _, err := buf.AllocateDocID("doc-1"); err != nil {
		t.Fatal(err)
	}
	buf.AddPosting("title", "hello", 0, 1, nil)
	buf.StoreField(0, "title", []byte("test"))

	buf.Reset()

	if buf.DocCount != 0 {
		t.Errorf("DocCount after reset = %d, want 0", buf.DocCount)
	}
	if buf.TermCount != 0 {
		t.Errorf("TermCount after reset = %d, want 0", buf.TermCount)
	}
	if len(buf.InvertedIndex) != 0 {
		t.Error("InvertedIndex should be empty after reset")
	}
}

func TestWriter_AddDocument(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	doc := Document{
		Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "Introduction to Search Engines",
			"body":  "Full-text search is a technique",
			"tags":  []interface{}{"search", "tutorial"},
		},
	}

	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}

	buf := w.Buffer()
	if buf.DocCount != 1 {
		t.Errorf("DocCount = %d, want 1", buf.DocCount)
	}

	// Check that title terms were indexed.
	titleIndex := buf.InvertedIndex["title"]
	if titleIndex == nil {
		t.Fatal("title field not indexed")
	}
	if _, ok := titleIndex["introduction"]; !ok {
		t.Error("expected 'introduction' in title index")
	}
	if _, ok := titleIndex["search"]; !ok {
		t.Error("expected 'search' in title index")
	}

	// Check keyword indexing.
	tagsIndex := buf.InvertedIndex["tags"]
	if tagsIndex == nil {
		t.Fatal("tags field not indexed")
	}
	if _, ok := tagsIndex["search"]; !ok {
		t.Error("expected 'search' in tags index")
	}
	if _, ok := tagsIndex["tutorial"]; !ok {
		t.Error("expected 'tutorial' in tags index")
	}

	// Check stored fields.
	stored := buf.StoredFields[0]
	if stored == nil {
		t.Fatal("no stored fields for doc 0")
	}
	if string(stored["title"]) != "Introduction to Search Engines" {
		t.Errorf("stored title = %q", stored["title"])
	}
}

func TestWriter_AddDocument_Positions(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	doc := Document{
		Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "quick brown fox",
		},
	}

	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}

	buf := w.Buffer()
	pl := buf.InvertedIndex["title"]["quick"]
	if pl == nil {
		t.Fatal("expected 'quick' in title index")
	}
	if len(pl.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(pl.Entries))
	}
	if len(pl.Entries[0].Positions) != 1 || pl.Entries[0].Positions[0] != 0 {
		t.Errorf("expected position [0], got %v", pl.Entries[0].Positions)
	}
}

func TestWriter_AddDocument_MissingID(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	doc := Document{
		Fields: map[string]interface{}{
			"title": "No ID",
		},
	}

	err := w.AddDocument(doc)
	if err == nil {
		t.Error("expected error for missing ID")
	}
}

func TestWriter_AddDocument_DuplicateID(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	doc := Document{
		Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "First",
		},
	}

	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}

	err := w.AddDocument(doc)
	if err != ErrDuplicateDoc {
		t.Errorf("expected ErrDuplicateDoc, got %v", err)
	}
}

func TestWriter_Abort(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	doc := Document{
		Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "Test",
		},
	}
	_ = w.AddDocument(doc)
	w.Abort()

	if w.Buffer().DocCount != 0 {
		t.Error("buffer should be empty after abort")
	}
}

func TestWriter_Release(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)
	w.Release()

	doc := Document{
		Fields: map[string]interface{}{
			"id":    "doc-1",
			"title": "Test",
		},
	}

	err := w.AddDocument(doc)
	if err != ErrWriterNotActive {
		t.Errorf("expected ErrWriterNotActive, got %v", err)
	}
}

func TestWriter_MultipleDocuments(t *testing.T) {
	schema := testSchema()
	registry := analysis.NewRegistry()
	w := NewWriter(schema, registry)

	docs := []Document{
		{Fields: map[string]interface{}{"id": "1", "title": "First Document"}},
		{Fields: map[string]interface{}{"id": "2", "title": "Second Document"}},
		{Fields: map[string]interface{}{"id": "3", "title": "Third Document"}},
	}

	for _, doc := range docs {
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	buf := w.Buffer()
	if buf.DocCount != 3 {
		t.Errorf("DocCount = %d, want 3", buf.DocCount)
	}

	// "document" should appear in all 3 docs.
	pl := buf.InvertedIndex["title"]["document"]
	if pl == nil {
		t.Fatal("expected 'document' in title index")
	}
	if len(pl.Entries) != 3 {
		t.Errorf("'document' entries = %d, want 3", len(pl.Entries))
	}
}
