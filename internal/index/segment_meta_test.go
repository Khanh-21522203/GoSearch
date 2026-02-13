package index

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testSegmentInfo() *SegmentInfo {
	return &SegmentInfo{
		SegmentID:  "seg_gen_42_e5f6g7h8",
		Generation: 42,
		CreatedAt:  time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		DocCount:   500,
		FieldStats: map[string]FieldStats{
			"title": {
				TermCount:     1250,
				TotalTermFreq: 3500,
				DocCount:      500,
				SumDocFreq:    2800,
			},
			"body": {
				TermCount:     8500,
				TotalTermFreq: 125000,
				DocCount:      500,
				SumDocFreq:    45000,
			},
		},
	}
}

func TestMarshalUnmarshalSegmentInfo_RoundTrip(t *testing.T) {
	info := testSegmentInfo()

	data, err := MarshalSegmentInfo(info)
	if err != nil {
		t.Fatal(err)
	}

	if info.Checksum == "" {
		t.Error("checksum should be populated after marshal")
	}

	got, err := UnmarshalSegmentInfo(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.SegmentID != info.SegmentID {
		t.Errorf("SegmentID = %s, want %s", got.SegmentID, info.SegmentID)
	}
	if got.Generation != info.Generation {
		t.Errorf("Generation = %d, want %d", got.Generation, info.Generation)
	}
	if got.DocCount != info.DocCount {
		t.Errorf("DocCount = %d, want %d", got.DocCount, info.DocCount)
	}
	if len(got.FieldStats) != 2 {
		t.Fatalf("FieldStats length = %d, want 2", len(got.FieldStats))
	}
	if got.FieldStats["title"].TermCount != 1250 {
		t.Errorf("title.TermCount = %d, want 1250", got.FieldStats["title"].TermCount)
	}
}

func TestUnmarshalSegmentInfo_Tampered(t *testing.T) {
	info := testSegmentInfo()
	data, err := MarshalSegmentInfo(info)
	if err != nil {
		t.Fatal(err)
	}

	tampered := make([]byte, len(data))
	copy(tampered, data)
	// Change a digit to invalidate checksum.
	for i := range tampered {
		if tampered[i] == '5' {
			tampered[i] = '9'
			break
		}
	}

	_, err = UnmarshalSegmentInfo(tampered)
	if err == nil {
		t.Error("expected error for tampered segment info")
	}
}

func TestWriteLoadSegmentInfo_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	info := testSegmentInfo()

	if err := WriteSegmentInfo(dir, info); err != nil {
		t.Fatal(err)
	}

	// Verify file exists.
	path := filepath.Join(dir, "meta.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("meta.json not found: %v", err)
	}

	loaded, err := LoadSegmentInfo(dir)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.SegmentID != info.SegmentID {
		t.Errorf("SegmentID = %s, want %s", loaded.SegmentID, info.SegmentID)
	}
	if loaded.DocCount != info.DocCount {
		t.Errorf("DocCount = %d, want %d", loaded.DocCount, info.DocCount)
	}
}

func TestSegmentFormatConstants(t *testing.T) {
	// Verify magic numbers are 8 bytes.
	for _, magic := range []string{MagicFST, MagicPostings, MagicPositions, MagicStored, MagicDeletions} {
		if len(magic) != 8 {
			t.Errorf("magic number %q length = %d, want 8", magic, len(magic))
		}
	}
}

func TestSizeLimits(t *testing.T) {
	if MaxTermLength != 32*1024 {
		t.Errorf("MaxTermLength = %d, want %d", MaxTermLength, 32*1024)
	}
	if MaxDocsPerSegment != 1<<31 {
		t.Errorf("MaxDocsPerSegment = %d, want %d", MaxDocsPerSegment, 1<<31)
	}
	if MaxSegmentSize != 4<<30 {
		t.Errorf("MaxSegmentSize = %d, want %d", MaxSegmentSize, 4<<30)
	}
}
