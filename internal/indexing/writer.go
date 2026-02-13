package indexing

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"GoSearch/internal/analysis"
	"GoSearch/internal/index"
)

var (
	ErrWriterLocked = errors.New("writer is already held by another caller")
)

// Document represents a JSON document to be indexed.
type Document struct {
	Fields map[string]interface{}
}

// Writer is the exclusive writer for a single index.
// Only one Writer may be active per index at any time.
type Writer struct {
	schema   *index.Schema
	registry *analysis.Registry
	buffer   *WriteBuffer

	mu     sync.Mutex
	active bool
}

// NewWriter creates a new Writer for the given schema and analyzer registry.
func NewWriter(schema *index.Schema, registry *analysis.Registry) *Writer {
	return &Writer{
		schema:   schema,
		registry: registry,
		buffer:   NewWriteBuffer(),
		active:   true,
	}
}

// AddDocument validates and indexes a single document into the write buffer.
func (w *Writer) AddDocument(doc Document) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.active {
		return ErrWriterNotActive
	}

	// Extract external ID.
	externalID, err := extractExternalID(doc)
	if err != nil {
		return err
	}

	// Allocate internal doc ID.
	docID, err := w.buffer.AllocateDocID(externalID)
	if err != nil {
		return err
	}

	// Process each field according to schema.
	for _, fieldDef := range w.schema.Fields {
		val, exists := doc.Fields[fieldDef.Name]
		if !exists {
			continue
		}

		switch fieldDef.Type {
		case index.FieldTypeText:
			if err := w.indexTextField(fieldDef, docID, val); err != nil {
				return err
			}
		case index.FieldTypeKeyword:
			if err := w.indexKeywordField(fieldDef, docID, val); err != nil {
				return err
			}
		case index.FieldTypeStoredOnly:
			// Store only, no indexing.
		}

		// Store field value if configured.
		if fieldDef.Stored {
			data, err := marshalFieldValue(val)
			if err != nil {
				return err
			}
			w.buffer.StoreField(docID, fieldDef.Name, data)
		}
	}

	return nil
}

// AddDocuments validates and indexes multiple documents into the write buffer.
func (w *Writer) AddDocuments(docs []Document) error {
	for i, doc := range docs {
		if err := w.AddDocument(doc); err != nil {
			return fmt.Errorf("document %d: %w", i, err)
		}
	}
	return nil
}

// DeleteDocument marks a document for deletion by external ID.
// The deletion is recorded in the write buffer and applied at commit time.
func (w *Writer) DeleteDocument(externalID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.active {
		return ErrWriterNotActive
	}

	w.buffer.MarkDeleted(externalID)
	return nil
}

// DocCount returns the number of documents currently in the write buffer.
func (w *Writer) DocCount() int {
	return w.buffer.DocCount
}

// IsFull returns true if the write buffer has reached its memory or document limit.
func (w *Writer) IsFull() bool {
	return w.buffer.IsFull()
}

// Buffer returns the current write buffer (for segment building).
func (w *Writer) Buffer() *WriteBuffer {
	return w.buffer
}

// Abort discards all buffered changes.
func (w *Writer) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer.Reset()
}

// Release releases the writer lock.
func (w *Writer) Release() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active = false
}

func (w *Writer) indexTextField(fieldDef index.FieldDef, docID uint32, val interface{}) error {
	text, ok := val.(string)
	if !ok {
		return errors.New("text field value must be a string")
	}

	analyzerName := fieldDef.Analyzer
	if analyzerName == "" {
		analyzerName = w.schema.DefaultAnalyzer
	}
	if analyzerName == "" {
		analyzerName = "standard"
	}

	analyzer, err := w.registry.Get(analyzerName)
	if err != nil {
		return err
	}

	tokens := analyzer.Analyze(fieldDef.Name, text)

	// Build term frequencies and positions.
	termFreqs := make(map[string]uint32)
	termPositions := make(map[string][]uint32)
	for _, tok := range tokens {
		termFreqs[tok.Term]++
		if fieldDef.Positions {
			termPositions[tok.Term] = append(termPositions[tok.Term], uint32(tok.Position))
		}
	}

	for term, freq := range termFreqs {
		var positions []uint32
		if fieldDef.Positions {
			positions = termPositions[term]
		}
		w.buffer.AddPosting(fieldDef.Name, term, docID, freq, positions)
	}

	return nil
}

func (w *Writer) indexKeywordField(fieldDef index.FieldDef, docID uint32, val interface{}) error {
	switch v := val.(type) {
	case string:
		w.buffer.AddPosting(fieldDef.Name, v, docID, 1, nil)
	case []interface{}:
		if !fieldDef.MultiValued {
			return errors.New("field is not multi-valued but received array")
		}
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return errors.New("keyword array values must be strings")
			}
			w.buffer.AddPosting(fieldDef.Name, s, docID, 1, nil)
		}
	default:
		return errors.New("keyword field value must be a string or string array")
	}
	return nil
}

func extractExternalID(doc Document) (string, error) {
	idVal, ok := doc.Fields["id"]
	if !ok {
		return "", errors.New("document missing 'id' field")
	}
	id, ok := idVal.(string)
	if !ok {
		return "", errors.New("document 'id' must be a string")
	}
	return id, nil
}

func marshalFieldValue(val interface{}) ([]byte, error) {
	switch v := val.(type) {
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}
