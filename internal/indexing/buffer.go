package indexing

import (
	"errors"
	"sync/atomic"
)

// Buffer limits.
const (
	DefaultBufferMemoryLimit = 64 * 1024 * 1024 // 64MB
	DefaultMaxDocsPerSegment = 100_000
)

var (
	ErrBufferFull       = errors.New("write buffer memory limit reached")
	ErrDuplicateDoc     = errors.New("duplicate document ID in buffer")
	ErrUnknownField     = errors.New("unknown field in document")
	ErrWriterNotActive  = errors.New("writer is not active")
)

// PostingEntry represents a single posting for a term in a field.
type PostingEntry struct {
	DocID     uint32
	Freq      uint32
	Positions []uint32
}

// PostingsList accumulates postings for a single term in a single field.
type PostingsList struct {
	Entries []PostingEntry
}

// WriteBuffer accumulates documents before commit.
// It builds an in-memory inverted index and stored fields map.
type WriteBuffer struct {
	// invertedIndex: field → term → postings list
	InvertedIndex map[string]map[string]*PostingsList

	// storedFields: docID → field → value
	StoredFields map[uint32]map[string][]byte

	// externalToInternal maps external doc IDs to internal doc IDs.
	ExternalToInternal map[string]uint32

	// Deletions tracks external IDs marked for deletion.
	Deletions map[string]bool

	NextDocID uint32
	DocCount  int
	TermCount int

	memoryUsed atomic.Int64
	MemoryLimit int64
	MaxDocs     int
}

// NewWriteBuffer creates a new empty write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{
		InvertedIndex:      make(map[string]map[string]*PostingsList),
		StoredFields:       make(map[uint32]map[string][]byte),
		ExternalToInternal: make(map[string]uint32),
		Deletions:          make(map[string]bool),
		MemoryLimit:        DefaultBufferMemoryLimit,
		MaxDocs:            DefaultMaxDocsPerSegment,
	}
}

// AddPosting adds a posting entry for the given field and term.
func (b *WriteBuffer) AddPosting(field, term string, docID uint32, freq uint32, positions []uint32) {
	fieldMap, ok := b.InvertedIndex[field]
	if !ok {
		fieldMap = make(map[string]*PostingsList)
		b.InvertedIndex[field] = fieldMap
	}

	pl, ok := fieldMap[term]
	if !ok {
		pl = &PostingsList{}
		fieldMap[term] = pl
		b.TermCount++
	}

	pl.Entries = append(pl.Entries, PostingEntry{
		DocID:     docID,
		Freq:      freq,
		Positions: positions,
	})

	// Approximate memory tracking.
	b.memoryUsed.Add(int64(16 + len(positions)*4))
}

// StoreField stores a field value for a document.
func (b *WriteBuffer) StoreField(docID uint32, field string, value []byte) {
	fields, ok := b.StoredFields[docID]
	if !ok {
		fields = make(map[string][]byte)
		b.StoredFields[docID] = fields
	}
	fields[field] = value
	b.memoryUsed.Add(int64(len(value) + len(field)))
}

// AllocateDocID assigns an internal doc ID for an external ID.
// Returns an error if the external ID is already in the buffer.
func (b *WriteBuffer) AllocateDocID(externalID string) (uint32, error) {
	if _, exists := b.ExternalToInternal[externalID]; exists {
		return 0, ErrDuplicateDoc
	}

	docID := b.NextDocID
	b.NextDocID++
	b.DocCount++
	b.ExternalToInternal[externalID] = docID
	return docID, nil
}

// MemoryUsed returns the approximate memory used by the buffer.
func (b *WriteBuffer) MemoryUsed() int64 {
	return b.memoryUsed.Load()
}

// IsFull returns true if the buffer has reached its memory or document limit.
func (b *WriteBuffer) IsFull() bool {
	if b.DocCount >= b.MaxDocs {
		return true
	}
	if b.memoryUsed.Load() >= b.MemoryLimit {
		return true
	}
	return false
}

// MarkDeleted records an external ID for deletion at commit time.
func (b *WriteBuffer) MarkDeleted(externalID string) {
	b.Deletions[externalID] = true
}

// Reset clears the buffer for reuse.
func (b *WriteBuffer) Reset() {
	b.InvertedIndex = make(map[string]map[string]*PostingsList)
	b.StoredFields = make(map[uint32]map[string][]byte)
	b.ExternalToInternal = make(map[string]uint32)
	b.Deletions = make(map[string]bool)
	b.NextDocID = 0
	b.DocCount = 0
	b.TermCount = 0
	b.memoryUsed.Store(0)
}
