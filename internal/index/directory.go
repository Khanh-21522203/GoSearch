package index

import (
	"fmt"
	"path/filepath"

	"GoSearch/internal/storage"
)

// IndexDir represents the on-disk directory layout for a single index.
// All path methods are pure functions with no I/O side effects.
type IndexDir struct {
	Root string
}

// NewIndexDir creates an IndexDir for the given root path.
func NewIndexDir(root string) *IndexDir {
	return &IndexDir{Root: root}
}

// SegmentsDir returns the path to the segments/ directory.
func (d *IndexDir) SegmentsDir() string {
	return filepath.Join(d.Root, "segments")
}

// ManifestsDir returns the path to the manifests/ directory.
func (d *IndexDir) ManifestsDir() string {
	return filepath.Join(d.Root, "manifests")
}

// TmpDir returns the path to the tmp/ directory.
func (d *IndexDir) TmpDir() string {
	return filepath.Join(d.Root, "tmp")
}

// ManifestCurrentPath returns the path to manifest.current.
func (d *IndexDir) ManifestCurrentPath() string {
	return filepath.Join(d.Root, "manifest.current")
}

// SchemaPath returns the path to schema.json.
func (d *IndexDir) SchemaPath() string {
	return filepath.Join(d.Root, "schema.json")
}

// SegmentDir returns the path to a specific segment's directory.
func (d *IndexDir) SegmentDir(segmentID string) string {
	return filepath.Join(d.Root, "segments", segmentID)
}

// SegmentFile returns the path to a specific file within a segment directory.
func (d *IndexDir) SegmentFile(segmentID, fileName string) string {
	return filepath.Join(d.Root, "segments", segmentID, fileName)
}

// TmpSegmentDir returns the path for a segment being built in tmp/.
func (d *IndexDir) TmpSegmentDir(segmentID string) string {
	return filepath.Join(d.Root, "tmp", segmentID)
}

// ManifestPath returns the path for a specific manifest generation file.
func (d *IndexDir) ManifestPath(generation uint64) string {
	return filepath.Join(d.Root, "manifests", fmt.Sprintf("manifest_gen_%d.json", generation))
}

// TmpManifestPath returns the path for a manifest being written in tmp/.
func (d *IndexDir) TmpManifestPath(generation uint64) string {
	return filepath.Join(d.Root, "tmp", fmt.Sprintf("manifest_gen_%d.json", generation))
}

// ManifestNextPath returns the path for manifest.next in tmp/.
func (d *IndexDir) ManifestNextPath() string {
	return filepath.Join(d.Root, "tmp", "manifest.next")
}

// EnsureDirectories creates all required subdirectories if they do not exist.
func (d *IndexDir) EnsureDirectories() error {
	for _, dir := range []string{d.SegmentsDir(), d.ManifestsDir(), d.TmpDir()} {
		if err := storage.EnsureDir(dir); err != nil {
			return fmt.Errorf("ensure directory %s: %w", dir, err)
		}
	}
	return nil
}

// SegmentFileNames returns the well-known segment file names.
func SegmentFileNames() []string {
	return []string{
		"meta.json",
		"fst.bin",
		"postings.bin",
		"positions.bin",
		"stored.bin",
		"deletions.bin",
	}
}
