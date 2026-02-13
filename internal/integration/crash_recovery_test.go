package integration

import (
	"os"
	"path/filepath"
	"testing"

	"GoSearch/internal/index"
	"GoSearch/internal/recovery"
	"GoSearch/internal/storage"
	"GoSearch/internal/testutil"
)

func TestCrashRecovery_TempFilesCleanup(t *testing.T) {
	testutil.WithTempDir(t, func(dir string) {
		idxDir := testutil.CreateTestIndexDir(t, dir)

		// Leave orphan files in tmp/.
		tmpDir := idxDir.TmpDir()
		os.MkdirAll(tmpDir, 0755)
		orphanFile := filepath.Join(tmpDir, "orphan_segment")
		os.MkdirAll(orphanFile, 0755)
		os.WriteFile(filepath.Join(orphanFile, "data.bin"), []byte("orphan"), 0644)

		// Run recovery.
		result, err := recovery.Recover(idxDir, recovery.DefaultOptions())
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}

		// tmp/ should be cleaned.
		entries, _ := os.ReadDir(tmpDir)
		if len(entries) != 0 {
			t.Errorf("tmp/ should be empty after recovery, got %d entries", len(entries))
		}

		_ = result
	})
}

func TestCrashRecovery_CorruptManifest(t *testing.T) {
	testutil.WithTempDir(t, func(dir string) {
		idxDir := testutil.CreateTestIndexDir(t, dir)

		// Write a valid generation 1 manifest.
		m1 := &index.Manifest{
			Generation: 1,
			Segments:   nil,
		}
		if err := index.WriteManifest(idxDir, m1); err != nil {
			t.Fatalf("WriteManifest gen 1: %v", err)
		}
		if err := index.WriteCurrentGeneration(idxDir, 1); err != nil {
			t.Fatalf("WriteCurrentGeneration 1: %v", err)
		}

		// Write a corrupt generation 2 manifest.
		m2Path := idxDir.ManifestPath(2)
		os.WriteFile(m2Path, []byte("corrupt data"), 0644)
		if err := index.WriteCurrentGeneration(idxDir, 2); err != nil {
			t.Fatalf("WriteCurrentGeneration 2: %v", err)
		}

		// Recovery should fall back to generation 1.
		result, err := recovery.Recover(idxDir, recovery.DefaultOptions())
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}

		if result.Generation != 1 {
			t.Errorf("recovered generation = %d, want 1", result.Generation)
		}
	})
}

func TestCrashRecovery_EmptyIndex(t *testing.T) {
	testutil.WithTempDir(t, func(dir string) {
		idxDir := testutil.CreateTestIndexDir(t, dir)

		result, err := recovery.Recover(idxDir, recovery.DefaultOptions())
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}

		if result.Generation != 0 {
			t.Errorf("recovered generation = %d, want 0", result.Generation)
		}
	})
}

func TestCrashRecovery_ValidCommit(t *testing.T) {
	testutil.WithTempDir(t, func(dir string) {
		idxDir := testutil.CreateTestIndexDir(t, dir)

		// Create a valid segment directory.
		segID := "seg_gen_1_abc"
		segDir := idxDir.SegmentDir(segID)
		os.MkdirAll(segDir, 0755)

		// Write a minimal segment file.
		metaContent := []byte(`{"test": true}`)
		os.WriteFile(filepath.Join(segDir, "meta.json"), metaContent, 0644)

		// Write manifest referencing the segment.
		checksum := storage.ComputeChecksum(metaContent)
		m := &index.Manifest{
			Generation: 1,
			Segments: []index.SegmentMeta{
				{
					ID:                segID,
					GenerationCreated: 1,
					Files: map[string]index.FileMeta{
						"meta.json": {Size: int64(len(metaContent)), Checksum: checksum},
					},
				},
			},
		}
		if err := index.WriteManifest(idxDir, m); err != nil {
			t.Fatalf("WriteManifest: %v", err)
		}
		if err := index.WriteCurrentGeneration(idxDir, 1); err != nil {
			t.Fatalf("WriteCurrentGeneration: %v", err)
		}

		result, err := recovery.Recover(idxDir, recovery.DefaultOptions())
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}

		if result.Generation != 1 {
			t.Errorf("recovered generation = %d, want 1", result.Generation)
		}
	})
}
