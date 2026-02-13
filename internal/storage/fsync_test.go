package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFsyncFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := FsyncFile(path); err != nil {
		t.Errorf("FsyncFile: %v", err)
	}
}

func TestFsyncFile_NotExists(t *testing.T) {
	err := FsyncFile("/nonexistent/path/file")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestFsyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := FsyncDir(dir); err != nil {
		t.Errorf("FsyncDir: %v", err)
	}
}

func TestFsyncDir_NotExists(t *testing.T) {
	err := FsyncDir("/nonexistent/path/dir")
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

func TestAtomicWriteFile(t *testing.T) {
	dir := t.TempDir()
	tmpDir := filepath.Join(dir, "tmp")
	if err := os.Mkdir(tmpDir, 0755); err != nil {
		t.Fatal(err)
	}

	finalPath := filepath.Join(dir, "final.txt")
	data := []byte("atomic write content")

	if err := AtomicWriteFile(finalPath, data, tmpDir); err != nil {
		t.Fatal(err)
	}

	// Verify final file has correct contents.
	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("file content = %q, want %q", got, data)
	}

	// Verify no temp files remain.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("tmp dir has %d entries, want 0", len(entries))
	}
}

func TestAtomicWriteFile_Overwrite(t *testing.T) {
	dir := t.TempDir()
	tmpDir := filepath.Join(dir, "tmp")
	if err := os.Mkdir(tmpDir, 0755); err != nil {
		t.Fatal(err)
	}

	finalPath := filepath.Join(dir, "file.txt")

	// Write initial content.
	if err := AtomicWriteFile(finalPath, []byte("first"), tmpDir); err != nil {
		t.Fatal(err)
	}

	// Overwrite.
	if err := AtomicWriteFile(finalPath, []byte("second"), tmpDir); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Errorf("file content = %q, want %q", got, "second")
	}
}

func TestWriteFileSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synced.txt")
	data := []byte("synced content")

	if err := WriteFileSync(path, data, FilePerm); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("file content = %q, want %q", got, data)
	}
}

func TestEnsureDir(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b", "c")

	if err := EnsureDir(nested); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(nested)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
}

func TestEnsureDir_AlreadyExists(t *testing.T) {
	dir := t.TempDir()
	// Should not error on existing dir.
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicWriteFileFromTemp(t *testing.T) {
	dir := t.TempDir()
	tmpPath := filepath.Join(dir, "tmp_file")
	finalPath := filepath.Join(dir, "final_file")

	data := []byte("from temp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	if err := AtomicWriteFileFromTemp(tmpPath, finalPath); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("file content = %q, want %q", got, data)
	}

	// Temp file should no longer exist.
	if FileExists(tmpPath) {
		t.Error("temp file should have been renamed")
	}
}
