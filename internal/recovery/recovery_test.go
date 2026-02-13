package recovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"GoSearch/internal/commit"
	"GoSearch/internal/index"
	"GoSearch/internal/storage"
)

func setupTestIndex(t *testing.T) *index.IndexDir {
	t.Helper()
	root := t.TempDir()
	dir := index.NewIndexDir(root)
	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func doCommit(t *testing.T, dir *index.IndexDir, currentManifest *index.Manifest) *commit.CommitResult {
	t.Helper()
	c := commit.NewCommitter(dir, commit.DefaultOptions())
	data := &commit.SegmentData{
		Files: map[string][]byte{
			"fst.bin":      []byte("fst-data"),
			"postings.bin": []byte("postings-data"),
		},
		DocCount:      10,
		DocCountAlive: 10,
	}
	result, err := c.Commit(context.Background(), currentManifest, data)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRecover_EmptyIndex(t *testing.T) {
	dir := setupTestIndex(t)

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 0 {
		t.Errorf("generation = %d, want 0", result.Generation)
	}
	if result.Manifest != nil {
		t.Error("manifest should be nil for empty index")
	}
	if result.FellBack {
		t.Error("should not have fallen back")
	}
}

func TestRecover_CleanState(t *testing.T) {
	dir := setupTestIndex(t)

	// Perform a commit.
	doCommit(t, dir, nil)

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 1 {
		t.Errorf("generation = %d, want 1", result.Generation)
	}
	if result.Manifest == nil {
		t.Fatal("manifest should not be nil")
	}
	if len(result.Manifest.Segments) != 1 {
		t.Errorf("segments = %d, want 1", len(result.Manifest.Segments))
	}
	if result.FellBack {
		t.Error("should not have fallen back on clean state")
	}
}

func TestRecover_MultipleCommits(t *testing.T) {
	dir := setupTestIndex(t)

	r1 := doCommit(t, dir, nil)
	m1, _ := index.LoadManifest(dir, r1.Generation)
	doCommit(t, dir, m1)

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 2 {
		t.Errorf("generation = %d, want 2", result.Generation)
	}
	if len(result.Manifest.Segments) != 2 {
		t.Errorf("segments = %d, want 2", len(result.Manifest.Segments))
	}
}

func TestRecover_TmpCleanup(t *testing.T) {
	dir := setupTestIndex(t)
	doCommit(t, dir, nil)

	// Leave junk in tmp/.
	if err := os.WriteFile(filepath.Join(dir.TmpDir(), "orphan.tmp"), []byte("junk"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir.TmpDir(), "leftover_dir"), 0755); err != nil {
		t.Fatal(err)
	}

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TmpFilesRemoved) != 2 {
		t.Errorf("TmpFilesRemoved = %d, want 2", len(result.TmpFilesRemoved))
	}

	// Verify tmp/ is now clean.
	entries, _ := os.ReadDir(dir.TmpDir())
	if len(entries) != 0 {
		t.Errorf("tmp/ has %d entries, want 0", len(entries))
	}
}

func TestRecover_OrphanSegmentCleanup(t *testing.T) {
	dir := setupTestIndex(t)
	doCommit(t, dir, nil)

	// Create an orphan segment directory.
	orphanDir := dir.SegmentDir("seg_gen_99_deadbeef")
	if err := os.MkdirAll(orphanDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanDir, "fst.bin"), []byte("orphan"), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.OrphansRemoved) != 1 {
		t.Errorf("OrphansRemoved = %d, want 1", len(result.OrphansRemoved))
	}
	if result.OrphansRemoved[0] != "seg_gen_99_deadbeef" {
		t.Errorf("orphan = %s, want seg_gen_99_deadbeef", result.OrphansRemoved[0])
	}

	// Verify orphan is gone.
	if storage.DirExists(orphanDir) {
		t.Error("orphan segment should have been removed")
	}
}

func TestRecover_CorruptManifestFallback(t *testing.T) {
	dir := setupTestIndex(t)

	// Commit gen 1.
	r1 := doCommit(t, dir, nil)
	m1, _ := index.LoadManifest(dir, r1.Generation)

	// Commit gen 2.
	doCommit(t, dir, m1)

	// Corrupt manifest gen 2.
	path := dir.ManifestPath(2)
	if err := os.WriteFile(path, []byte(`{"corrupt":true}`), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 1 {
		t.Errorf("generation = %d, want 1 (fallback)", result.Generation)
	}
	if !result.FellBack {
		t.Error("should have fallen back")
	}
	if result.FellBackFrom != 2 {
		t.Errorf("FellBackFrom = %d, want 2", result.FellBackFrom)
	}
}

func TestRecover_CorruptSegmentFallback(t *testing.T) {
	dir := setupTestIndex(t)

	// Commit gen 1.
	r1 := doCommit(t, dir, nil)
	m1, _ := index.LoadManifest(dir, r1.Generation)

	// Commit gen 2.
	r2 := doCommit(t, dir, m1)

	// Corrupt a file in the gen 2 segment.
	fstPath := dir.SegmentFile(r2.SegmentID, "fst.bin")
	if err := os.WriteFile(fstPath, []byte("corrupted!"), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 1 {
		t.Errorf("generation = %d, want 1 (fallback due to corrupt segment)", result.Generation)
	}
	if !result.FellBack {
		t.Error("should have fallen back")
	}
}

func TestRecover_AllCorrupt(t *testing.T) {
	dir := setupTestIndex(t)

	// Commit gen 1.
	r1 := doCommit(t, dir, nil)

	// Corrupt the only segment.
	fstPath := dir.SegmentFile(r1.SegmentID, "fst.bin")
	if err := os.WriteFile(fstPath, []byte("corrupted!"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Recover(dir, DefaultOptions())
	if err == nil {
		t.Error("expected error when all segments are corrupt")
	}
	if !errors.Is(err, ErrRecoveryImpossible) {
		t.Errorf("expected ErrRecoveryImpossible, got: %v", err)
	}
}

func TestRecover_OldManifestPruning(t *testing.T) {
	dir := setupTestIndex(t)

	// Create 5 sequential commits.
	var m *index.Manifest
	for i := 0; i < 5; i++ {
		r := doCommit(t, dir, m)
		m, _ = index.LoadManifest(dir, r.Generation)
	}

	// With default retention=2, should keep gen 5, 4, 3 and remove 1, 2.
	opts := DefaultOptions()
	result, err := Recover(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	if result.Generation != 5 {
		t.Errorf("generation = %d, want 5", result.Generation)
	}

	// Should have removed gen 1 and 2.
	if len(result.ManifestsRemoved) != 2 {
		t.Errorf("ManifestsRemoved = %d, want 2", len(result.ManifestsRemoved))
	}

	// Verify gen 1 and 2 manifests no longer exist.
	for _, gen := range []uint64{1, 2} {
		if storage.FileExists(dir.ManifestPath(gen)) {
			t.Errorf("manifest gen %d should have been removed", gen)
		}
	}

	// Verify gen 3, 4, 5 still exist.
	for _, gen := range []uint64{3, 4, 5} {
		if !storage.FileExists(dir.ManifestPath(gen)) {
			t.Errorf("manifest gen %d should still exist", gen)
		}
	}
}

func TestRecover_SkipChecksumVerification(t *testing.T) {
	dir := setupTestIndex(t)

	r := doCommit(t, dir, nil)

	// Corrupt a segment file.
	fstPath := dir.SegmentFile(r.SegmentID, "fst.bin")
	if err := os.WriteFile(fstPath, []byte("corrupted!"), 0644); err != nil {
		t.Fatal(err)
	}

	// With checksum verification disabled, should succeed.
	opts := DefaultOptions()
	opts.VerifySegmentChecksums = false

	result, err := Recover(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 1 {
		t.Errorf("generation = %d, want 1", result.Generation)
	}
	if result.FellBack {
		t.Error("should not have fallen back with checksums disabled")
	}
}

func TestRecover_MissingSegmentDir(t *testing.T) {
	dir := setupTestIndex(t)

	r := doCommit(t, dir, nil)

	// Remove the segment directory entirely.
	os.RemoveAll(dir.SegmentDir(r.SegmentID))

	_, err := Recover(dir, DefaultOptions())
	if err == nil {
		t.Error("expected error when segment directory is missing")
	}
}

func TestParseManifestGeneration(t *testing.T) {
	tests := []struct {
		filename string
		gen      uint64
		ok       bool
	}{
		{"manifest_gen_1.json", 1, true},
		{"manifest_gen_42.json", 42, true},
		{"manifest_gen_0.json", 0, true},
		{"manifest_gen_100.json", 100, true},
		{"other_file.json", 0, false},
		{"manifest_gen_.json", 0, false},
		{"manifest_gen_abc.json", 0, false},
	}

	for _, tt := range tests {
		gen, ok := parseManifestGeneration(tt.filename)
		if ok != tt.ok {
			t.Errorf("parseManifestGeneration(%q) ok = %v, want %v", tt.filename, ok, tt.ok)
		}
		if ok && gen != tt.gen {
			t.Errorf("parseManifestGeneration(%q) gen = %d, want %d", tt.filename, gen, tt.gen)
		}
	}
}

func TestRecover_EmptyIndex_TmpHasFiles(t *testing.T) {
	dir := setupTestIndex(t)

	// No manifest.current, but leave junk in tmp/.
	if err := os.WriteFile(filepath.Join(dir.TmpDir(), "leftover"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := Recover(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 0 {
		t.Errorf("generation = %d, want 0", result.Generation)
	}
	if len(result.TmpFilesRemoved) != 1 {
		t.Errorf("TmpFilesRemoved = %d, want 1", len(result.TmpFilesRemoved))
	}
}
