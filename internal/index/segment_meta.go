package index

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"GoSearch/internal/storage"
)

// Segment file magic numbers (8 bytes each).
const (
	MagicFST       = "GTSRFST\x00"
	MagicPostings  = "GTSRPST\x00"
	MagicPositions = "GTSRPOS\x00"
	MagicStored    = "GTSRSTO\x00"
	MagicDeletions = "GTSRDEL\x00"
)

// Segment file format version.
const SegmentFormatVersion uint32 = 1

// Size limits.
const (
	MaxTermLength     = 32 * 1024 // 32KB UTF-8 bytes
	MaxDocsPerSegment = 1 << 31   // 2^31 (int32 doc ID)
	MaxSegmentSize    = 4 << 30   // 4GB (uint32 offsets)
)

// SegmentInfo is the on-disk meta.json for a segment.
type SegmentInfo struct {
	SegmentID  string                `json:"segment_id"`
	Generation uint64                `json:"generation"`
	CreatedAt  time.Time             `json:"created_at"`
	DocCount   uint32                `json:"doc_count"`
	FieldStats map[string]FieldStats `json:"field_stats"`
	Checksum   storage.Checksum      `json:"checksum"`
}

// FieldStats contains per-field statistics for a segment.
type FieldStats struct {
	TermCount    uint64 `json:"term_count"`
	TotalTermFreq uint64 `json:"total_term_freq"`
	DocCount     uint32 `json:"doc_count"`
	SumDocFreq   uint64 `json:"sum_doc_freq"`
}

// MarshalSegmentInfo serializes segment info to JSON with a checksum.
func MarshalSegmentInfo(info *SegmentInfo) ([]byte, error) {
	checksum, err := computeSegmentInfoChecksum(info)
	if err != nil {
		return nil, fmt.Errorf("compute segment info checksum: %w", err)
	}
	info.Checksum = checksum

	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal segment info: %w", err)
	}
	return data, nil
}

// UnmarshalSegmentInfo deserializes segment info from JSON and verifies its checksum.
func UnmarshalSegmentInfo(data []byte) (*SegmentInfo, error) {
	var info SegmentInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unmarshal segment info: %w", err)
	}

	savedChecksum := info.Checksum
	computed, err := computeSegmentInfoChecksum(&info)
	if err != nil {
		return nil, fmt.Errorf("compute segment info checksum for verification: %w", err)
	}
	if computed != savedChecksum {
		return nil, fmt.Errorf("segment info checksum mismatch: expected %s, got %s", savedChecksum, computed)
	}

	return &info, nil
}

// WriteSegmentInfo writes a segment's meta.json to the given directory.
func WriteSegmentInfo(segDir string, info *SegmentInfo) error {
	data, err := MarshalSegmentInfo(info)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/meta.json", segDir)
	if err := storage.WriteFileSync(path, data, storage.FilePerm); err != nil {
		return fmt.Errorf("write segment info: %w", err)
	}
	return nil
}

// LoadSegmentInfo reads and verifies a segment's meta.json.
func LoadSegmentInfo(segDir string) (*SegmentInfo, error) {
	path := fmt.Sprintf("%s/meta.json", segDir)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read segment info: %w", err)
	}
	return UnmarshalSegmentInfo(data)
}

func computeSegmentInfoChecksum(info *SegmentInfo) (storage.Checksum, error) {
	saved := info.Checksum
	info.Checksum = ""
	defer func() { info.Checksum = saved }()

	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal for checksum: %w", err)
	}
	return storage.ComputeChecksum(data), nil
}
