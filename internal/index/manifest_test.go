package index

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"GoSearch/internal/storage"
)

func TestMarshalUnmarshalManifest_RoundTrip(t *testing.T) {
	m := &Manifest{
		Generation:         5,
		PreviousGeneration: 4,
		Timestamp:          time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		CommitID:           "test-commit-id",
		Segments: []SegmentMeta{
			{
				ID:                "seg_gen_5_abcd1234",
				GenerationCreated: 5,
				DocCount:          100,
				DocCountAlive:     95,
				DelCount:          5,
				SizeBytes:         1024,
				MinDocID:          0,
				MaxDocID:          99,
				Files: map[string]FileMeta{
					"fst.bin":      {Size: 512, Checksum: storage.ComputeChecksum([]byte("fst"))},
					"postings.bin": {Size: 512, Checksum: storage.ComputeChecksum([]byte("postings"))},
				},
			},
		},
		SchemaVersion:  1,
		TotalDocs:      100,
		TotalDocsAlive: 95,
		TotalSizeBytes: 1024,
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's valid JSON.
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("marshaled manifest is not valid JSON: %v", err)
	}

	// Verify checksum field is populated.
	if m.Checksum == "" {
		t.Error("checksum should be populated after marshal")
	}

	// Unmarshal and verify.
	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Generation != m.Generation {
		t.Errorf("Generation = %d, want %d", got.Generation, m.Generation)
	}
	if got.CommitID != m.CommitID {
		t.Errorf("CommitID = %s, want %s", got.CommitID, m.CommitID)
	}
	if len(got.Segments) != 1 {
		t.Fatalf("Segments length = %d, want 1", len(got.Segments))
	}
	if got.Segments[0].ID != "seg_gen_5_abcd1234" {
		t.Errorf("Segment ID = %s, want seg_gen_5_abcd1234", got.Segments[0].ID)
	}
}

func TestUnmarshalManifest_TamperedData(t *testing.T) {
	m := &Manifest{
		Generation: 1,
		CommitID:   "test",
		Segments:   []SegmentMeta{},
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with the data.
	tampered := make([]byte, len(data))
	copy(tampered, data)
	// Change a byte in the middle.
	for i := range tampered {
		if tampered[i] == '1' {
			tampered[i] = '2'
			break
		}
	}

	_, err = UnmarshalManifest(tampered)
	if err == nil {
		t.Error("expected error for tampered manifest")
	}
	if !errors.Is(err, ErrManifestCorrupt) {
		t.Errorf("expected ErrManifestCorrupt, got: %v", err)
	}
}

func TestEmptyManifest(t *testing.T) {
	m := EmptyManifest()
	if m.Generation != 0 {
		t.Errorf("Generation = %d, want 0", m.Generation)
	}
	if len(m.Segments) != 0 {
		t.Errorf("Segments length = %d, want 0", len(m.Segments))
	}
}

func TestMarshalManifest_DeterministicSegmentOrder(t *testing.T) {
	m := &Manifest{
		Generation: 3,
		Segments: []SegmentMeta{
			{ID: "seg_c"},
			{ID: "seg_a"},
			{ID: "seg_b"},
		},
	}

	data1, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	// Reorder and marshal again.
	m.Checksum = ""
	m.Segments = []SegmentMeta{
		{ID: "seg_b"},
		{ID: "seg_c"},
		{ID: "seg_a"},
	}

	data2, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	if string(data1) != string(data2) {
		t.Error("manifest serialization should be deterministic regardless of segment input order")
	}
}

func TestUnmarshalManifestNoVerify(t *testing.T) {
	m := &Manifest{
		Generation: 1,
		CommitID:   "test",
		Segments:   []SegmentMeta{},
		Checksum:   "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// This would fail with UnmarshalManifest (wrong checksum).
	got, err := UnmarshalManifestNoVerify(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 {
		t.Errorf("Generation = %d, want 1", got.Generation)
	}
}

func TestMarshalManifest_EmptySegments(t *testing.T) {
	m := &Manifest{
		Generation: 1,
		CommitID:   "empty",
		Segments:   []SegmentMeta{},
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Segments) != 0 {
		t.Errorf("Segments length = %d, want 0", len(got.Segments))
	}
}
