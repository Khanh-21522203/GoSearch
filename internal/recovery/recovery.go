package recovery

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"GoSearch/internal/index"
	"GoSearch/internal/storage"
)

// ErrRecoveryImpossible is returned when no valid manifest with intact
// segments can be found.
var ErrRecoveryImpossible = errors.New("recovery impossible: no valid manifest with intact segments found")

// RecoveryResult contains the outcome of crash recovery.
type RecoveryResult struct {
	// Generation is the recovered generation number (0 if empty index).
	Generation uint64

	// Manifest is the validated manifest for the recovered generation.
	// Nil if Generation is 0.
	Manifest *index.Manifest

	// OrphansRemoved lists segment IDs that were removed as orphans.
	OrphansRemoved []string

	// ManifestsRemoved lists manifest generation numbers that were pruned.
	ManifestsRemoved []uint64

	// TmpFilesRemoved lists paths removed from tmp/.
	TmpFilesRemoved []string

	// FellBack is true if recovery fell back to an earlier manifest.
	FellBack bool

	// FellBackFrom is the generation that was corrupt (only set if FellBack).
	FellBackFrom uint64
}

// Recover executes the 9-step crash recovery protocol.
// This must be called during index startup, before the index accepts reads or writes.
func Recover(dir *index.IndexDir, opts Options) (*RecoveryResult, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	result := &RecoveryResult{}

	// Step 1: Read current generation
	generation, err := step1ReadGeneration(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("recovery step 1 (read generation): %w", err)
	}

	// Generation 0: empty index, just clean up.
	if generation == 0 {
		logger.Info("recovery: empty index (generation 0)")
		removed, _ := step5CleanTmp(dir, logger)
		result.TmpFilesRemoved = removed
		result.Generation = 0
		return result, nil
	}

	// Step 2: Load manifest
	manifest, actualGen, fellBack, err := step2LoadManifest(dir, generation, logger)
	if err != nil {
		return nil, fmt.Errorf("recovery step 2 (load manifest): %w", err)
	}
	if fellBack {
		result.FellBack = true
		result.FellBackFrom = generation
		generation = actualGen
	}

	// Step 3: Verify segments
	corruptSegments, err := step3VerifySegments(dir, manifest, opts.VerifySegmentChecksums, logger)
	if err != nil {
		return nil, fmt.Errorf("recovery step 3 (verify segments): %w", err)
	}

	// Step 4: Handle corrupt segments
	if len(corruptSegments) > 0 {
		prevGen := generation
		manifest, generation, err = step4HandleCorruptSegments(dir, generation, opts.VerifySegmentChecksums, logger)
		if err != nil {
			return nil, fmt.Errorf("recovery step 4 (handle corrupt segments): %w", err)
		}
		result.FellBack = true
		result.FellBackFrom = prevGen

		// Update manifest.current so the next startup uses the recovered generation.
		if err := index.WriteCurrentGeneration(dir, generation); err != nil {
			return nil, fmt.Errorf("recovery step 4 (update manifest.current): %w", err)
		}
		logger.Info("recovery step 4: updated manifest.current", "generation", generation)
	}

	// Step 5: Clean tmp/
	removed, err := step5CleanTmp(dir, logger)
	if err != nil {
		logger.Warn("recovery step 5: non-fatal error cleaning tmp", "error", err)
	}
	result.TmpFilesRemoved = removed

	// Step 6: Identify orphans
	orphans, err := step6IdentifyOrphans(dir, manifest, logger)
	if err != nil {
		logger.Warn("recovery step 6: non-fatal error identifying orphans", "error", err)
	}

	// Step 7: Clean orphans
	if len(orphans) > 0 {
		if err := step7CleanOrphans(dir, orphans, logger); err != nil {
			logger.Warn("recovery step 7: non-fatal error cleaning orphans", "error", err)
		}
		result.OrphansRemoved = orphans
	}

	// Step 8: Clean old manifests
	removedManifests, err := step8CleanOldManifests(dir, generation, opts.ManifestRetention, logger)
	if err != nil {
		logger.Warn("recovery step 8: non-fatal error cleaning manifests", "error", err)
	}
	result.ManifestsRemoved = removedManifests

	// Step 9: Finalize
	result.Generation = generation
	result.Manifest = manifest

	logger.Info("recovery complete",
		"generation", generation,
		"segments", len(manifest.Segments),
		"orphans_removed", len(result.OrphansRemoved),
		"manifests_removed", len(result.ManifestsRemoved),
	)

	return result, nil
}

func step1ReadGeneration(dir *index.IndexDir, logger *slog.Logger) (uint64, error) {
	gen, err := index.ReadCurrentGeneration(dir)
	if err != nil {
		return 0, err
	}
	logger.Info("recovery step 1: read generation", "generation", gen)
	return gen, nil
}

func step2LoadManifest(dir *index.IndexDir, generation uint64, logger *slog.Logger) (*index.Manifest, uint64, bool, error) {
	logger.Info("recovery step 2: load manifest", "generation", generation)

	m, actualGen, err := index.LoadManifestWithFallback(dir, generation, logger)
	if err != nil {
		return nil, 0, false, err
	}
	fellBack := actualGen != generation
	return m, actualGen, fellBack, nil
}

func step3VerifySegments(dir *index.IndexDir, manifest *index.Manifest, verifyChecksums bool, logger *slog.Logger) ([]string, error) {
	logger.Info("recovery step 3: verify segments",
		"count", len(manifest.Segments),
		"verify_checksums", verifyChecksums,
	)

	var corrupt []string
	for _, seg := range manifest.Segments {
		segDir := dir.SegmentDir(seg.ID)
		if !storage.DirExists(segDir) {
			logger.Error("segment directory missing", "segment", seg.ID, "path", segDir)
			corrupt = append(corrupt, seg.ID)
			continue
		}

		if verifyChecksums {
			for fileName, fileMeta := range seg.Files {
				path := dir.SegmentFile(seg.ID, fileName)
				if err := storage.VerifyFileChecksum(path, fileMeta.Checksum); err != nil {
					logger.Error("segment file checksum mismatch",
						"segment", seg.ID,
						"file", fileName,
						"error", err,
					)
					corrupt = append(corrupt, seg.ID)
					break // One bad file is enough to mark segment corrupt.
				}
			}
		}
	}
	return corrupt, nil
}

func step4HandleCorruptSegments(dir *index.IndexDir, currentGen uint64, verifyChecksums bool, logger *slog.Logger) (*index.Manifest, uint64, error) {
	logger.Warn("recovery step 4: handling corrupt segments, trying earlier manifests")

	for gen := currentGen - 1; gen >= 1; gen-- {
		m, err := index.LoadManifest(dir, gen)
		if err != nil {
			logger.Warn("earlier manifest load failed", "generation", gen, "error", err)
			continue
		}

		corrupt, err := step3VerifySegments(dir, m, verifyChecksums, logger)
		if err != nil {
			continue
		}
		if len(corrupt) == 0 {
			logger.Info("recovery: fell back to earlier generation", "generation", gen)
			return m, gen, nil
		}
	}

	return nil, 0, ErrRecoveryImpossible
}

func step5CleanTmp(dir *index.IndexDir, logger *slog.Logger) ([]string, error) {
	removed, err := storage.RemoveDirContents(dir.TmpDir())
	if len(removed) > 0 {
		logger.Info("recovery step 5: cleaned tmp", "removed", len(removed))
		for _, p := range removed {
			logger.Debug("removed tmp entry", "path", p)
		}
	}
	return removed, err
}

func step6IdentifyOrphans(dir *index.IndexDir, manifest *index.Manifest, logger *slog.Logger) ([]string, error) {
	onDisk, err := storage.ListSubdirs(dir.SegmentsDir())
	if err != nil {
		return nil, err
	}

	referenced := make(map[string]bool, len(manifest.Segments))
	for _, seg := range manifest.Segments {
		referenced[seg.ID] = true
	}

	var orphans []string
	for _, name := range onDisk {
		if !referenced[name] {
			logger.Info("recovery step 6: identified orphan segment", "segment", name)
			orphans = append(orphans, name)
		}
	}
	return orphans, nil
}

func step7CleanOrphans(dir *index.IndexDir, orphans []string, logger *slog.Logger) error {
	var firstErr error
	for _, segID := range orphans {
		path := dir.SegmentDir(segID)
		if err := os.RemoveAll(path); err != nil {
			logger.Error("failed to remove orphan segment", "segment", segID, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		logger.Info("recovery step 7: removed orphan segment", "segment", segID)
	}
	return firstErr
}

func step8CleanOldManifests(dir *index.IndexDir, currentGen uint64, retention int, logger *slog.Logger) ([]uint64, error) {
	files, err := storage.ListFiles(dir.ManifestsDir())
	if err != nil {
		return nil, err
	}

	// Parse generation numbers from manifest filenames.
	var generations []uint64
	for _, f := range files {
		gen, ok := parseManifestGeneration(f)
		if ok {
			generations = append(generations, gen)
		}
	}

	// Sort descending.
	sort.Slice(generations, func(i, j int) bool {
		return generations[i] > generations[j]
	})

	// Keep current + retention predecessors.
	keep := 1 + retention
	if keep > len(generations) {
		return nil, nil // Nothing to prune.
	}

	toRemove := generations[keep:]
	var removed []uint64
	for _, gen := range toRemove {
		path := dir.ManifestPath(gen)
		if err := os.Remove(path); err != nil {
			logger.Warn("failed to remove old manifest", "generation", gen, "error", err)
			continue
		}
		logger.Info("recovery step 8: removed old manifest", "generation", gen)
		removed = append(removed, gen)
	}
	return removed, nil
}

// parseManifestGeneration extracts the generation number from a manifest filename.
// Expected format: manifest_gen_N.json
func parseManifestGeneration(filename string) (uint64, bool) {
	name := strings.TrimSuffix(filename, filepath.Ext(filename))
	if !strings.HasPrefix(name, "manifest_gen_") {
		return 0, false
	}
	numStr := name[len("manifest_gen_"):]
	gen, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}
