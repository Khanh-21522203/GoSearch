package commit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"GoSearch/internal/index"
	"GoSearch/internal/storage"
)

// SegmentData represents the output of a segment builder.
// Files maps logical file names (e.g., "fst.bin") to their content bytes.
type SegmentData struct {
	Files         map[string][]byte
	DocCount      uint32
	DocCountAlive uint32
	DelCount      uint32
	MinDocID      uint64
	MaxDocID      uint64
}

// CommitResult contains information about a successful commit.
type CommitResult struct {
	Generation uint64
	SegmentID  string
	CommitID   string
	Duration   time.Duration
}

// Committer orchestrates the 7-phase commit protocol.
type Committer struct {
	dir    *index.IndexDir
	opts   Options
	logger *slog.Logger
}

// NewCommitter creates a new Committer for the given index directory.
func NewCommitter(dir *index.IndexDir, opts Options) *Committer {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Committer{
		dir:    dir,
		opts:   opts,
		logger: logger,
	}
}

// Commit executes the full 7-phase commit protocol.
//
// The caller must hold an exclusive write lock on the index.
// currentManifest may be nil for the first commit (treated as empty manifest).
func (c *Committer) Commit(ctx context.Context, currentManifest *index.Manifest, segmentData *SegmentData) (*CommitResult, error) {
	start := time.Now()

	if currentManifest == nil {
		currentManifest = index.EmptyManifest()
	}

	newGeneration := currentManifest.Generation + 1

	// Phase 1: PREPARE
	c.logger.Info("commit phase 1: prepare", "generation", newGeneration)
	segmentID, segMeta, commitID, err := c.phase1Prepare(newGeneration, segmentData)
	if err != nil {
		return nil, fmt.Errorf("commit phase 1 (prepare): %w", err)
	}

	// Phase 2: WRITE
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("commit cancelled before phase 2: %w", err)
	}
	c.logger.Info("commit phase 2: write", "segment", segmentID)
	if err := c.phase2Write(segmentID, segmentData); err != nil {
		c.rollback(segmentID)
		return nil, fmt.Errorf("commit phase 2 (write): %w", err)
	}

	// Phase 3: VERIFY
	if err := ctx.Err(); err != nil {
		c.rollback(segmentID)
		return nil, fmt.Errorf("commit cancelled before phase 3: %w", err)
	}
	c.logger.Info("commit phase 3: verify", "segment", segmentID)
	if err := c.phase3Verify(segmentID, segMeta.Files); err != nil {
		c.rollback(segmentID)
		return nil, fmt.Errorf("commit phase 3 (verify): %w", err)
	}

	// Phase 4: INSTALL
	c.logger.Info("commit phase 4: install", "segment", segmentID)
	if err := c.phase4Install(segmentID); err != nil {
		c.rollback(segmentID)
		return nil, fmt.Errorf("commit phase 4 (install): %w", err)
	}

	// Phase 5: MANIFEST
	c.logger.Info("commit phase 5: manifest", "generation", newGeneration)
	newManifest := c.buildManifest(currentManifest, newGeneration, segMeta, commitID)
	if err := c.phase5Manifest(newManifest); err != nil {
		return nil, fmt.Errorf("commit phase 5 (manifest): %w", err)
	}

	// Phase 6: ACTIVATION
	c.logger.Info("commit phase 6: activation", "generation", newGeneration)
	if err := c.phase6Activation(newGeneration); err != nil {
		return nil, fmt.Errorf("commit phase 6 (activation): %w", err)
	}

	// Phase 7: CLEANUP
	c.logger.Info("commit phase 7: cleanup")
	if err := c.phase7Cleanup(); err != nil {
		c.logger.Warn("commit phase 7 (cleanup) non-fatal error", "error", err)
	}

	duration := time.Since(start)
	c.logger.Info("commit complete",
		"generation", newGeneration,
		"segment", segmentID,
		"duration", duration,
	)

	return &CommitResult{
		Generation: newGeneration,
		SegmentID:  segmentID,
		CommitID:   commitID,
		Duration:   duration,
	}, nil
}

// phase1Prepare generates segment ID, computes checksums for all files,
// and builds the SegmentMeta.
func (c *Committer) phase1Prepare(generation uint64, data *SegmentData) (string, index.SegmentMeta, string, error) {
	segmentID, err := generateSegmentID(generation)
	if err != nil {
		return "", index.SegmentMeta{}, "", fmt.Errorf("generate segment ID: %w", err)
	}

	commitID, err := generateCommitID()
	if err != nil {
		return "", index.SegmentMeta{}, "", fmt.Errorf("generate commit ID: %w", err)
	}

	files := make(map[string]index.FileMeta, len(data.Files))
	var totalSize uint64
	for name, content := range data.Files {
		checksum := storage.ComputeChecksum(content)
		size := int64(len(content))
		files[name] = index.FileMeta{
			Size:     size,
			Checksum: checksum,
		}
		totalSize += uint64(size)
	}

	meta := index.SegmentMeta{
		ID:                segmentID,
		GenerationCreated: generation,
		DocCount:          data.DocCount,
		DocCountAlive:     data.DocCountAlive,
		DelCount:          data.DelCount,
		SizeBytes:         totalSize,
		MinDocID:          data.MinDocID,
		MaxDocID:          data.MaxDocID,
		Files:             files,
	}

	return segmentID, meta, commitID, nil
}

// phase2Write creates the segment directory in tmp/ and writes all files with fsync.
func (c *Committer) phase2Write(segmentID string, data *SegmentData) error {
	segDir := c.dir.TmpSegmentDir(segmentID)
	if err := storage.EnsureDir(segDir); err != nil {
		return fmt.Errorf("create tmp segment dir: %w", err)
	}

	for name, content := range data.Files {
		path := filepath.Join(segDir, name)
		if err := storage.WriteFileSync(path, content, storage.FilePerm); err != nil {
			return fmt.Errorf("write segment file %s: %w", name, err)
		}
	}

	// fsync the segment directory to ensure all file entries are durable.
	if err := storage.FsyncDir(segDir); err != nil {
		return fmt.Errorf("fsync segment dir: %w", err)
	}

	return nil
}

// phase3Verify re-reads each file from tmp/ and verifies checksums.
func (c *Committer) phase3Verify(segmentID string, expectedFiles map[string]index.FileMeta) error {
	segDir := c.dir.TmpSegmentDir(segmentID)
	for name, meta := range expectedFiles {
		path := filepath.Join(segDir, name)
		if err := storage.VerifyFileChecksum(path, meta.Checksum); err != nil {
			return fmt.Errorf("verify segment file %s: %w", name, err)
		}
	}
	return nil
}

// phase4Install renames the segment directory from tmp/ to segments/.
func (c *Committer) phase4Install(segmentID string) error {
	src := c.dir.TmpSegmentDir(segmentID)
	dst := c.dir.SegmentDir(segmentID)

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("rename segment %s → %s: %w", src, dst, err)
	}
	if err := storage.FsyncDir(c.dir.SegmentsDir()); err != nil {
		return fmt.Errorf("fsync segments dir: %w", err)
	}
	return nil
}

// phase5Manifest builds and writes the new manifest.
func (c *Committer) phase5Manifest(m *index.Manifest) error {
	return index.WriteManifest(c.dir, m)
}

// phase6Activation writes the new generation to manifest.current.
func (c *Committer) phase6Activation(generation uint64) error {
	return index.WriteCurrentGeneration(c.dir, generation)
}

// phase7Cleanup removes tmp/ contents.
func (c *Committer) phase7Cleanup() error {
	removed, err := storage.RemoveDirContents(c.dir.TmpDir())
	if len(removed) > 0 {
		c.logger.Debug("cleanup removed tmp files", "count", len(removed))
	}
	return err
}

// rollback cleans up tmp/ artifacts after a failed commit.
func (c *Committer) rollback(segmentID string) {
	segDir := c.dir.TmpSegmentDir(segmentID)
	if err := os.RemoveAll(segDir); err != nil {
		c.logger.Warn("rollback: failed to remove tmp segment dir", "path", segDir, "error", err)
	}
}

// buildManifest creates a new manifest incorporating the new segment.
func (c *Committer) buildManifest(prev *index.Manifest, gen uint64, newSeg index.SegmentMeta, commitID string) *index.Manifest {
	segments := make([]index.SegmentMeta, 0, len(prev.Segments)+1)
	segments = append(segments, prev.Segments...)
	segments = append(segments, newSeg)

	var totalDocs, totalAlive, totalSize uint64
	for _, s := range segments {
		totalDocs += uint64(s.DocCount)
		totalAlive += uint64(s.DocCountAlive)
		totalSize += s.SizeBytes
	}

	return &index.Manifest{
		Generation:         gen,
		PreviousGeneration: prev.Generation,
		Timestamp:          time.Now().UTC(),
		CommitID:           commitID,
		Segments:           segments,
		SchemaVersion:      c.opts.SchemaVersion,
		TotalDocs:          totalDocs,
		TotalDocsAlive:     totalAlive,
		TotalSizeBytes:     totalSize,
	}
}

// generateSegmentID creates a segment ID: seg_gen_<N>_<8-hex-chars>.
func generateSegmentID(generation uint64) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random bytes: %w", err)
	}
	return fmt.Sprintf("seg_gen_%d_%s", generation, hex.EncodeToString(b)), nil
}

// generateCommitID creates a UUID-like commit identifier.
func generateCommitID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}
