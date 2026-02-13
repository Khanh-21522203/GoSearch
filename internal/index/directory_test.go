package index

import (
	"path/filepath"
	"testing"

	"GoSearch/internal/storage"
)

func TestIndexDir_Paths(t *testing.T) {
	dir := NewIndexDir("/data/indexes/myindex")

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"SegmentsDir", dir.SegmentsDir(), "/data/indexes/myindex/segments"},
		{"ManifestsDir", dir.ManifestsDir(), "/data/indexes/myindex/manifests"},
		{"TmpDir", dir.TmpDir(), "/data/indexes/myindex/tmp"},
		{"ManifestCurrentPath", dir.ManifestCurrentPath(), "/data/indexes/myindex/manifest.current"},
		{"SchemaPath", dir.SchemaPath(), "/data/indexes/myindex/schema.json"},
		{"SegmentDir", dir.SegmentDir("seg_gen_1_abc"), "/data/indexes/myindex/segments/seg_gen_1_abc"},
		{"SegmentFile", dir.SegmentFile("seg_gen_1_abc", "fst.bin"), "/data/indexes/myindex/segments/seg_gen_1_abc/fst.bin"},
		{"TmpSegmentDir", dir.TmpSegmentDir("seg_gen_1_abc"), "/data/indexes/myindex/tmp/seg_gen_1_abc"},
		{"ManifestPath", dir.ManifestPath(42), "/data/indexes/myindex/manifests/manifest_gen_42.json"},
		{"TmpManifestPath", dir.TmpManifestPath(42), "/data/indexes/myindex/tmp/manifest_gen_42.json"},
		{"ManifestNextPath", dir.ManifestNextPath(), "/data/indexes/myindex/tmp/manifest.next"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %s, want %s", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestIndexDir_EnsureDirectories(t *testing.T) {
	root := t.TempDir()
	dir := NewIndexDir(root)

	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}

	for _, subdir := range []string{dir.SegmentsDir(), dir.ManifestsDir(), dir.TmpDir()} {
		if !storage.DirExists(subdir) {
			t.Errorf("directory not created: %s", subdir)
		}
	}
}

func TestIndexDir_EnsureDirectories_Idempotent(t *testing.T) {
	root := t.TempDir()
	dir := NewIndexDir(root)

	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	// Calling again should not error.
	if err := dir.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentFileNames(t *testing.T) {
	names := SegmentFileNames()
	expected := map[string]bool{
		"meta.json":     true,
		"fst.bin":       true,
		"postings.bin":  true,
		"positions.bin": true,
		"stored.bin":    true,
		"deletions.bin": true,
	}

	if len(names) != len(expected) {
		t.Errorf("got %d file names, want %d", len(names), len(expected))
	}

	for _, name := range names {
		if !expected[name] {
			t.Errorf("unexpected segment file name: %s", name)
		}
	}
}

func TestNewIndexDir(t *testing.T) {
	dir := NewIndexDir("/some/path")
	if dir.Root != "/some/path" {
		t.Errorf("Root = %s, want /some/path", dir.Root)
	}
}

func TestIndexDir_ManifestPath_Generations(t *testing.T) {
	dir := NewIndexDir("/data")
	tests := []struct {
		gen  uint64
		want string
	}{
		{0, "/data/manifests/manifest_gen_0.json"},
		{1, "/data/manifests/manifest_gen_1.json"},
		{100, "/data/manifests/manifest_gen_100.json"},
	}
	for _, tt := range tests {
		got := dir.ManifestPath(tt.gen)
		// Use filepath.Clean for comparison in case of OS-specific separators.
		if filepath.Clean(got) != filepath.Clean(tt.want) {
			t.Errorf("ManifestPath(%d) = %s, want %s", tt.gen, got, tt.want)
		}
	}
}
