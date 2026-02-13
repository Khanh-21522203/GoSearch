package commit

import (
	"context"
	"testing"

	"GoSearch/internal/index"
	"GoSearch/internal/storage"
)

func newTestCommitter(t *testing.T) (*Committer, *index.IndexDir) {
	t.Helper()
	root := t.TempDir()
	dir := index.NewIndexDir(root)
	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	c := NewCommitter(dir, DefaultOptions())
	return c, dir
}

func testSegmentData() *SegmentData {
	return &SegmentData{
		Files: map[string][]byte{
			"meta.json":    []byte(`{"segment_id":"test"}`),
			"fst.bin":      []byte("fst-data-here"),
			"postings.bin": []byte("postings-data-here"),
		},
		DocCount:      10,
		DocCountAlive: 10,
		DelCount:      0,
		MinDocID:      0,
		MaxDocID:      9,
	}
}

func TestCommit_FirstCommit(t *testing.T) {
	c, dir := newTestCommitter(t)
	ctx := context.Background()

	result, err := c.Commit(ctx, nil, testSegmentData())
	if err != nil {
		t.Fatal(err)
	}

	if result.Generation != 1 {
		t.Errorf("Generation = %d, want 1", result.Generation)
	}
	if result.SegmentID == "" {
		t.Error("SegmentID should not be empty")
	}
	if result.CommitID == "" {
		t.Error("CommitID should not be empty")
	}
	if result.Duration <= 0 {
		t.Error("Duration should be positive")
	}

	// Verify segment directory exists in segments/.
	segDir := dir.SegmentDir(result.SegmentID)
	if !storage.DirExists(segDir) {
		t.Errorf("segment directory not found: %s", segDir)
	}

	// Verify segment files exist.
	for _, name := range []string{"meta.json", "fst.bin", "postings.bin"} {
		path := dir.SegmentFile(result.SegmentID, name)
		if !storage.FileExists(path) {
			t.Errorf("segment file not found: %s", path)
		}
	}

	// Verify manifest exists.
	m, err := index.LoadManifest(dir, 1)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if m.Generation != 1 {
		t.Errorf("manifest generation = %d, want 1", m.Generation)
	}
	if len(m.Segments) != 1 {
		t.Fatalf("manifest segments = %d, want 1", len(m.Segments))
	}
	if m.Segments[0].ID != result.SegmentID {
		t.Errorf("manifest segment ID = %s, want %s", m.Segments[0].ID, result.SegmentID)
	}

	// Verify manifest.current.
	gen, err := index.ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 1 {
		t.Errorf("current generation = %d, want 1", gen)
	}

	// Verify tmp/ is clean.
	files, _ := storage.ListFiles(dir.TmpDir())
	dirs, _ := storage.ListSubdirs(dir.TmpDir())
	if len(files)+len(dirs) != 0 {
		t.Errorf("tmp/ should be empty, has %d files and %d dirs", len(files), len(dirs))
	}
}

func TestCommit_SequentialCommits(t *testing.T) {
	c, dir := newTestCommitter(t)
	ctx := context.Background()

	// First commit.
	r1, err := c.Commit(ctx, nil, testSegmentData())
	if err != nil {
		t.Fatal(err)
	}

	// Load manifest from first commit.
	m1, err := index.LoadManifest(dir, r1.Generation)
	if err != nil {
		t.Fatal(err)
	}

	// Second commit.
	r2, err := c.Commit(ctx, m1, testSegmentData())
	if err != nil {
		t.Fatal(err)
	}

	if r2.Generation != 2 {
		t.Errorf("second generation = %d, want 2", r2.Generation)
	}

	// Verify manifest for gen 2 has both segments.
	m2, err := index.LoadManifest(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Segments) != 2 {
		t.Errorf("manifest gen 2 segments = %d, want 2", len(m2.Segments))
	}
	if m2.PreviousGeneration != 1 {
		t.Errorf("previous generation = %d, want 1", m2.PreviousGeneration)
	}

	// Both manifests should exist.
	if _, err := index.LoadManifest(dir, 1); err != nil {
		t.Errorf("manifest gen 1 should still exist: %v", err)
	}

	// Current generation should be 2.
	gen, _ := index.ReadCurrentGeneration(dir)
	if gen != 2 {
		t.Errorf("current gen = %d, want 2", gen)
	}
}

func TestCommit_ContextCancellation(t *testing.T) {
	c, dir := newTestCommitter(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := c.Commit(ctx, nil, testSegmentData())
	if err == nil {
		t.Error("expected error from cancelled context")
	}

	// Verify no generation was written.
	gen, _ := index.ReadCurrentGeneration(dir)
	if gen != 0 {
		t.Errorf("generation = %d, want 0 after cancelled commit", gen)
	}
}

func TestCommit_SegmentChecksums(t *testing.T) {
	c, dir := newTestCommitter(t)
	ctx := context.Background()

	data := &SegmentData{
		Files: map[string][]byte{
			"fst.bin": []byte("known-fst-content"),
		},
		DocCount:      5,
		DocCountAlive: 5,
	}

	result, err := c.Commit(ctx, nil, data)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the file checksum matches.
	expectedChecksum := storage.ComputeChecksum([]byte("known-fst-content"))
	m, _ := index.LoadManifest(dir, result.Generation)
	seg := m.Segments[0]

	fstMeta, ok := seg.Files["fst.bin"]
	if !ok {
		t.Fatal("fst.bin not found in segment files")
	}
	if fstMeta.Checksum != expectedChecksum {
		t.Errorf("fst.bin checksum = %s, want %s", fstMeta.Checksum, expectedChecksum)
	}

	// Verify the file on disk matches.
	if err := storage.VerifyFileChecksum(dir.SegmentFile(result.SegmentID, "fst.bin"), expectedChecksum); err != nil {
		t.Errorf("on-disk checksum verification failed: %v", err)
	}
}

func TestCommit_ManifestAggregates(t *testing.T) {
	c, dir := newTestCommitter(t)
	ctx := context.Background()

	data1 := &SegmentData{
		Files:         map[string][]byte{"fst.bin": make([]byte, 100)},
		DocCount:      10,
		DocCountAlive: 8,
		DelCount:      2,
	}
	r1, _ := c.Commit(ctx, nil, data1)
	m1, _ := index.LoadManifest(dir, r1.Generation)

	data2 := &SegmentData{
		Files:         map[string][]byte{"fst.bin": make([]byte, 200)},
		DocCount:      20,
		DocCountAlive: 20,
		DelCount:      0,
	}
	_, _ = c.Commit(ctx, m1, data2)
	m2, _ := index.LoadManifest(dir, 2)

	if m2.TotalDocs != 30 {
		t.Errorf("TotalDocs = %d, want 30", m2.TotalDocs)
	}
	if m2.TotalDocsAlive != 28 {
		t.Errorf("TotalDocsAlive = %d, want 28", m2.TotalDocsAlive)
	}
	if m2.TotalSizeBytes != 300 {
		t.Errorf("TotalSizeBytes = %d, want 300", m2.TotalSizeBytes)
	}
}

func TestCommit_SegmentIDFormat(t *testing.T) {
	c, _ := newTestCommitter(t)
	ctx := context.Background()

	result, err := c.Commit(ctx, nil, testSegmentData())
	if err != nil {
		t.Fatal(err)
	}

	// Segment ID should match pattern: seg_gen_1_<8 hex chars>
	id := result.SegmentID
	if len(id) < len("seg_gen_1_") {
		t.Fatalf("segment ID too short: %s", id)
	}
	prefix := id[:len("seg_gen_1_")]
	if prefix != "seg_gen_1_" {
		t.Errorf("segment ID prefix = %s, want seg_gen_1_", prefix)
	}
	suffix := id[len("seg_gen_1_"):]
	if len(suffix) != 8 {
		t.Errorf("segment ID suffix length = %d, want 8 hex chars", len(suffix))
	}
}

func TestGenerateSegmentID(t *testing.T) {
	id1, err := generateSegmentID(42)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := generateSegmentID(42)
	if err != nil {
		t.Fatal(err)
	}

	// IDs should be unique (different random suffix).
	if id1 == id2 {
		t.Error("generated segment IDs should be unique")
	}
}

func TestGenerateCommitID(t *testing.T) {
	id1, err := generateCommitID()
	if err != nil {
		t.Fatal(err)
	}
	id2, err := generateCommitID()
	if err != nil {
		t.Fatal(err)
	}

	if id1 == id2 {
		t.Error("generated commit IDs should be unique")
	}
	if len(id1) != 32 {
		t.Errorf("commit ID length = %d, want 32 hex chars", len(id1))
	}
}
