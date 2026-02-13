package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"GoSearch/internal/storage"
)

var ErrManifestCorrupt = errors.New("manifest checksum verification failed")

// Manifest represents a committed generation's manifest.
type Manifest struct {
	Generation         uint64           `json:"generation"`
	PreviousGeneration uint64           `json:"previous_generation"`
	Timestamp          time.Time        `json:"timestamp"`
	CommitID           string           `json:"commit_id"`
	Segments           []SegmentMeta    `json:"segments"`
	SchemaVersion      uint32           `json:"schema_version"`
	TotalDocs          uint64           `json:"total_docs"`
	TotalDocsAlive     uint64           `json:"total_docs_alive"`
	TotalSizeBytes     uint64           `json:"total_size_bytes"`
	Checksum           storage.Checksum `json:"checksum"`
}

// SegmentMeta describes a single segment within a manifest.
type SegmentMeta struct {
	ID                string                      `json:"id"`
	GenerationCreated uint64                      `json:"generation_created"`
	DocCount          uint32                      `json:"doc_count"`
	DocCountAlive     uint32                      `json:"doc_count_alive"`
	DelCount          uint32                      `json:"del_count"`
	SizeBytes         uint64                      `json:"size_bytes"`
	MinDocID          uint64                      `json:"min_doc_id"`
	MaxDocID          uint64                      `json:"max_doc_id"`
	Files             map[string]FileMeta         `json:"files"`
}

// FileMeta describes a single file within a segment.
type FileMeta struct {
	Size     int64            `json:"size"`
	Checksum storage.Checksum `json:"checksum"`
}

// EmptyManifest returns a manifest representing generation 0 (empty index).
func EmptyManifest() *Manifest {
	return &Manifest{
		Generation: 0,
		Segments:   []SegmentMeta{},
	}
}

// MarshalManifest serializes a manifest to JSON and computes its checksum.
// The checksum is computed over the JSON with the checksum field set to empty.
func MarshalManifest(m *Manifest) ([]byte, error) {
	// Ensure deterministic segment ordering.
	sortSegments(m.Segments)

	// Compute checksum over JSON with empty checksum field.
	checksum, err := computeManifestChecksum(m)
	if err != nil {
		return nil, fmt.Errorf("compute manifest checksum: %w", err)
	}
	m.Checksum = checksum

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return data, nil
}

// UnmarshalManifest deserializes a manifest from JSON and verifies its checksum.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	savedChecksum := m.Checksum
	computed, err := computeManifestChecksum(&m)
	if err != nil {
		return nil, fmt.Errorf("compute manifest checksum for verification: %w", err)
	}
	if computed != savedChecksum {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrManifestCorrupt, savedChecksum, computed)
	}

	return &m, nil
}

// UnmarshalManifestNoVerify deserializes a manifest without checking the checksum.
func UnmarshalManifestNoVerify(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return &m, nil
}

// computeManifestChecksum computes the checksum of a manifest by serializing
// it with an empty checksum field and computing SHA-256 over the result.
func computeManifestChecksum(m *Manifest) (storage.Checksum, error) {
	saved := m.Checksum
	m.Checksum = ""
	defer func() { m.Checksum = saved }()

	sortSegments(m.Segments)
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal for checksum: %w", err)
	}
	return storage.ComputeChecksum(data), nil
}

// sortSegments sorts segments by ID for deterministic serialization.
func sortSegments(segments []SegmentMeta) {
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})
}
