package index

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"GoSearch/internal/storage"
)

var ErrManifestNotFound = errors.New("manifest not found")

// ReadCurrentGeneration reads the generation number from manifest.current.
// Returns 0 if the file does not exist (empty index).
func ReadCurrentGeneration(dir *IndexDir) (uint64, error) {
	data, err := os.ReadFile(dir.ManifestCurrentPath())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read manifest.current: %w", err)
	}

	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0, nil
	}

	gen, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse manifest.current %q: %w", s, err)
	}
	return gen, nil
}

// WriteCurrentGeneration atomically writes a new generation number to manifest.current.
// Follows the activation protocol:
//  1. Write generation to tmp/manifest.next
//  2. fsync tmp/manifest.next
//  3. Rename to manifest.current
//  4. fsync index root directory
func WriteCurrentGeneration(dir *IndexDir, generation uint64) error {
	data := []byte(strconv.FormatUint(generation, 10))

	// Write to tmp/manifest.next with fsync.
	nextPath := dir.ManifestNextPath()
	if err := storage.WriteFileSync(nextPath, data, storage.FilePerm); err != nil {
		return fmt.Errorf("write manifest.next: %w", err)
	}

	// Rename to manifest.current.
	if err := os.Rename(nextPath, dir.ManifestCurrentPath()); err != nil {
		return fmt.Errorf("rename manifest.next → manifest.current: %w", err)
	}

	// fsync the index root directory.
	if err := storage.FsyncDir(dir.Root); err != nil {
		return fmt.Errorf("fsync index root: %w", err)
	}

	return nil
}

// LoadManifest reads and verifies a manifest file for the given generation.
func LoadManifest(dir *IndexDir, generation uint64) (*Manifest, error) {
	path := dir.ManifestPath(generation)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: generation %d", ErrManifestNotFound, generation)
		}
		return nil, fmt.Errorf("read manifest gen %d: %w", generation, err)
	}

	m, err := UnmarshalManifest(data)
	if err != nil {
		return nil, fmt.Errorf("manifest gen %d: %w", generation, err)
	}
	return m, nil
}

// WriteManifest atomically writes a manifest file.
// Follows the manifest protocol:
//  1. Marshal manifest to JSON (with checksum)
//  2. Write to tmp/manifest_gen_N.json
//  3. fsync the temp file
//  4. Rename to manifests/manifest_gen_N.json
//  5. fsync manifests/ directory
func WriteManifest(dir *IndexDir, m *Manifest) error {
	data, err := MarshalManifest(m)
	if err != nil {
		return fmt.Errorf("marshal manifest gen %d: %w", m.Generation, err)
	}

	tmpPath := dir.TmpManifestPath(m.Generation)
	if err := storage.WriteFileSync(tmpPath, data, storage.FilePerm); err != nil {
		return fmt.Errorf("write tmp manifest gen %d: %w", m.Generation, err)
	}

	finalPath := dir.ManifestPath(m.Generation)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename manifest gen %d: %w", m.Generation, err)
	}

	if err := storage.FsyncDir(dir.ManifestsDir()); err != nil {
		return fmt.Errorf("fsync manifests dir: %w", err)
	}

	return nil
}

// LoadManifestWithFallback attempts to load the manifest for generation N.
// If it is corrupt, it tries N-1, N-2, ... down to generation 1.
// Returns the manifest, its actual generation, and any error.
func LoadManifestWithFallback(dir *IndexDir, generation uint64, logger *slog.Logger) (*Manifest, uint64, error) {
	for gen := generation; gen >= 1; gen-- {
		m, err := LoadManifest(dir, gen)
		if err == nil {
			if gen != generation {
				logger.Warn("manifest fallback",
					"requested", generation,
					"recovered", gen,
				)
			}
			return m, gen, nil
		}

		logger.Warn("manifest load failed, trying previous",
			"generation", gen,
			"error", err,
		)
	}

	return nil, 0, fmt.Errorf("no valid manifest found for generations %d through 1", generation)
}
