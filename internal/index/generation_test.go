package index

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"GoSearch/internal/storage"
)

func newTestDir(t *testing.T) *IndexDir {
	t.Helper()
	root := t.TempDir()
	dir := NewIndexDir(root)
	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestReadCurrentGeneration_Missing(t *testing.T) {
	dir := newTestDir(t)
	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 0 {
		t.Errorf("gen = %d, want 0 for missing file", gen)
	}
}

func TestReadCurrentGeneration_Valid(t *testing.T) {
	dir := newTestDir(t)
	if err := os.WriteFile(dir.ManifestCurrentPath(), []byte("42"), 0644); err != nil {
		t.Fatal(err)
	}

	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 42 {
		t.Errorf("gen = %d, want 42", gen)
	}
}

func TestReadCurrentGeneration_WithTrailingNewline(t *testing.T) {
	dir := newTestDir(t)
	if err := os.WriteFile(dir.ManifestCurrentPath(), []byte("10\n"), 0644); err != nil {
		t.Fatal(err)
	}

	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 10 {
		t.Errorf("gen = %d, want 10", gen)
	}
}

func TestReadCurrentGeneration_Corrupt(t *testing.T) {
	dir := newTestDir(t)
	if err := os.WriteFile(dir.ManifestCurrentPath(), []byte("not-a-number"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ReadCurrentGeneration(dir)
	if err == nil {
		t.Error("expected error for corrupt manifest.current")
	}
}

func TestReadCurrentGeneration_Empty(t *testing.T) {
	dir := newTestDir(t)
	if err := os.WriteFile(dir.ManifestCurrentPath(), []byte(""), 0644); err != nil {
		t.Fatal(err)
	}

	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 0 {
		t.Errorf("gen = %d, want 0 for empty file", gen)
	}
}

func TestWriteCurrentGeneration_RoundTrip(t *testing.T) {
	dir := newTestDir(t)

	if err := WriteCurrentGeneration(dir, 99); err != nil {
		t.Fatal(err)
	}

	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 99 {
		t.Errorf("gen = %d, want 99", gen)
	}

	// Verify temp file was cleaned up.
	if storage.FileExists(dir.ManifestNextPath()) {
		t.Error("manifest.next should have been renamed")
	}
}

func TestWriteManifest_LoadManifest_RoundTrip(t *testing.T) {
	dir := newTestDir(t)

	m := &Manifest{
		Generation:         3,
		PreviousGeneration: 2,
		Timestamp:          time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		CommitID:           "commit-3",
		Segments: []SegmentMeta{
			{
				ID:                "seg_gen_3_abcdef01",
				GenerationCreated: 3,
				DocCount:          50,
				DocCountAlive:     50,
				SizeBytes:         2048,
				Files: map[string]FileMeta{
					"fst.bin": {Size: 1024, Checksum: storage.ComputeChecksum([]byte("fst"))},
				},
			},
		},
		SchemaVersion: 1,
		TotalDocs:     50,
		TotalDocsAlive: 50,
		TotalSizeBytes: 2048,
	}

	if err := WriteManifest(dir, m); err != nil {
		t.Fatal(err)
	}

	// Verify manifest file exists in manifests/.
	path := dir.ManifestPath(3)
	if !storage.FileExists(path) {
		t.Error("manifest file not found in manifests/")
	}

	// Verify tmp file was cleaned up.
	if storage.FileExists(dir.TmpManifestPath(3)) {
		t.Error("tmp manifest should have been renamed")
	}

	// Load and verify.
	loaded, err := LoadManifest(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Generation != 3 {
		t.Errorf("Generation = %d, want 3", loaded.Generation)
	}
	if loaded.CommitID != "commit-3" {
		t.Errorf("CommitID = %s, want commit-3", loaded.CommitID)
	}
}

func TestLoadManifest_NotExists(t *testing.T) {
	dir := newTestDir(t)
	_, err := LoadManifest(dir, 999)
	if err == nil {
		t.Error("expected error for missing manifest")
	}
	if !errors.Is(err, ErrManifestNotFound) {
		t.Errorf("expected ErrManifestNotFound, got: %v", err)
	}
}

func TestLoadManifestWithFallback(t *testing.T) {
	dir := newTestDir(t)
	logger := slog.Default()

	// Write valid manifest for gen 1.
	m1 := &Manifest{
		Generation: 1,
		CommitID:   "commit-1",
		Segments:   []SegmentMeta{},
	}
	if err := WriteManifest(dir, m1); err != nil {
		t.Fatal(err)
	}

	// Write corrupt manifest for gen 2.
	corruptPath := dir.ManifestPath(2)
	if err := os.WriteFile(corruptPath, []byte(`{"generation":2,"checksum":"sha256:wrong"}`), 0644); err != nil {
		t.Fatal(err)
	}

	// Fallback should find gen 1.
	m, gen, err := LoadManifestWithFallback(dir, 2, logger)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 1 {
		t.Errorf("generation = %d, want 1", gen)
	}
	if m.CommitID != "commit-1" {
		t.Errorf("CommitID = %s, want commit-1", m.CommitID)
	}
}

func TestLoadManifestWithFallback_AllCorrupt(t *testing.T) {
	dir := newTestDir(t)
	logger := slog.Default()

	// Write corrupt manifests for gen 1 and 2.
	for _, gen := range []uint64{1, 2} {
		path := dir.ManifestPath(gen)
		if err := os.WriteFile(path, []byte(`{"corrupt": true}`), 0644); err != nil {
			t.Fatal(err)
		}
	}

	_, _, err := LoadManifestWithFallback(dir, 2, logger)
	if err == nil {
		t.Error("expected error when all manifests are corrupt")
	}
}

func TestLoadManifestWithFallback_DirectHit(t *testing.T) {
	dir := newTestDir(t)
	logger := slog.Default()

	m := &Manifest{
		Generation: 5,
		CommitID:   "commit-5",
		Segments:   []SegmentMeta{},
	}
	if err := WriteManifest(dir, m); err != nil {
		t.Fatal(err)
	}

	loaded, gen, err := LoadManifestWithFallback(dir, 5, logger)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 5 {
		t.Errorf("generation = %d, want 5", gen)
	}
	if loaded.CommitID != "commit-5" {
		t.Errorf("CommitID = %s, want commit-5", loaded.CommitID)
	}
}

func TestWriteCurrentGeneration_Overwrite(t *testing.T) {
	dir := newTestDir(t)

	if err := WriteCurrentGeneration(dir, 1); err != nil {
		t.Fatal(err)
	}
	if err := WriteCurrentGeneration(dir, 2); err != nil {
		t.Fatal(err)
	}

	gen, err := ReadCurrentGeneration(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 2 {
		t.Errorf("gen = %d, want 2", gen)
	}
}

func TestLoadManifest_CorruptChecksum(t *testing.T) {
	dir := newTestDir(t)

	// Write a valid manifest.
	m := &Manifest{Generation: 1, CommitID: "ok", Segments: []SegmentMeta{}}
	if err := WriteManifest(dir, m); err != nil {
		t.Fatal(err)
	}

	// Tamper with the file on disk.
	path := dir.ManifestPath(1)
	data, _ := os.ReadFile(path)
	// Replace "ok" with "no" to invalidate checksum.
	tampered := make([]byte, len(data))
	copy(tampered, data)
	for i := 0; i < len(tampered)-1; i++ {
		if tampered[i] == 'o' && tampered[i+1] == 'k' {
			tampered[i] = 'n'
			tampered[i+1] = 'o'
			break
		}
	}
	os.WriteFile(path, tampered, 0644)

	_, err := LoadManifest(dir, 1)
	if err == nil {
		t.Error("expected error for tampered manifest")
	}
}

func TestManifestPath_Format(t *testing.T) {
	dir := newTestDir(t)
	path := dir.ManifestPath(42)
	expected := filepath.Join(dir.Root, "manifests", "manifest_gen_42.json")
	if path != expected {
		t.Errorf("ManifestPath(42) = %s, want %s", path, expected)
	}
}
