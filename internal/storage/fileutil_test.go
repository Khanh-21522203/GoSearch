package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveDirContents(t *testing.T) {
	dir := t.TempDir()

	// Create some files and a subdirectory.
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "subdir", "c.txt"), []byte("c"), 0644); err != nil {
		t.Fatal(err)
	}

	removed, err := RemoveDirContents(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 3 {
		t.Errorf("removed %d entries, want 3", len(removed))
	}

	// Directory itself should still exist.
	if !DirExists(dir) {
		t.Error("directory should still exist")
	}

	// Should be empty.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("dir has %d entries, want 0", len(entries))
	}
}

func TestRemoveDirContents_NotExists(t *testing.T) {
	removed, err := RemoveDirContents("/nonexistent/path")
	if err != nil {
		t.Errorf("expected nil error for non-existent dir, got: %v", err)
	}
	if len(removed) != 0 {
		t.Errorf("expected no removed entries, got %d", len(removed))
	}
}

func TestRemoveDirContents_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	removed, err := RemoveDirContents(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Errorf("removed %d entries from empty dir", len(removed))
	}
}

func TestListSubdirs(t *testing.T) {
	dir := t.TempDir()

	if err := os.Mkdir(filepath.Join(dir, "dir1"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "dir2"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("f"), 0644); err != nil {
		t.Fatal(err)
	}

	dirs, err := ListSubdirs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 2 {
		t.Errorf("got %d subdirs, want 2", len(dirs))
	}
}

func TestListSubdirs_NotExists(t *testing.T) {
	dirs, err := ListSubdirs("/nonexistent/path")
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
	if len(dirs) != 0 {
		t.Errorf("expected empty list, got %d", len(dirs))
	}
}

func TestListFiles(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.json"), []byte("b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}

	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Errorf("got %d files, want 2", len(files))
	}
}

func TestListFiles_NotExists(t *testing.T) {
	files, err := ListFiles("/nonexistent/path")
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected empty list, got %d", len(files))
	}
}

func TestFileExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exists.txt")
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if !FileExists(path) {
		t.Error("FileExists should return true for existing file")
	}
	if FileExists(filepath.Join(dir, "nope.txt")) {
		t.Error("FileExists should return false for non-existent file")
	}
	if FileExists(dir) {
		t.Error("FileExists should return false for directory")
	}
}

func TestDirExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if !DirExists(dir) {
		t.Error("DirExists should return true for existing directory")
	}
	if DirExists(path) {
		t.Error("DirExists should return false for file")
	}
	if DirExists(filepath.Join(dir, "nope")) {
		t.Error("DirExists should return false for non-existent path")
	}
}
