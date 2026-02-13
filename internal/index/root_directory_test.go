package index

import (
	"path/filepath"
	"testing"

	"GoSearch/internal/storage"
)

func TestRootDir_Paths(t *testing.T) {
	r := NewRootDir("/data")

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"IndexesDir", r.IndexesDir(), "/data/indexes"},
		{"GlobalDir", r.GlobalDir(), "/data/global"},
		{"ConfigPath", r.ConfigPath(), "/data/global/config.json"},
		{"LocksDir", r.LocksDir(), "/data/global/locks"},
		{"LockPath", r.LockPath("myindex"), "/data/global/locks/myindex.lock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %s, want %s", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestRootDir_IndexDir(t *testing.T) {
	r := NewRootDir("/data")
	dir := r.IndexDir("myindex")
	want := filepath.Join("/data", "indexes", "myindex")
	if dir.Root != want {
		t.Errorf("IndexDir.Root = %s, want %s", dir.Root, want)
	}
}

func TestRootDir_EnsureDirectories(t *testing.T) {
	root := t.TempDir()
	r := NewRootDir(root)

	if err := r.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}

	for _, dir := range []string{r.IndexesDir(), r.GlobalDir(), r.LocksDir()} {
		if !storage.DirExists(dir) {
			t.Errorf("directory not created: %s", dir)
		}
	}
}

func TestRootDir_EnsureDirectories_Idempotent(t *testing.T) {
	root := t.TempDir()
	r := NewRootDir(root)

	if err := r.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	if err := r.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
}

func TestRootDir_ListIndexes_Empty(t *testing.T) {
	root := t.TempDir()
	r := NewRootDir(root)
	if err := r.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}

	indexes, err := r.ListIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 0 {
		t.Errorf("expected 0 indexes, got %d", len(indexes))
	}
}

func TestRootDir_ListIndexes_WithIndexes(t *testing.T) {
	root := t.TempDir()
	r := NewRootDir(root)
	if err := r.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}

	// Create two index directories.
	for _, name := range []string{"index_a", "index_b"} {
		dir := r.IndexDir(name)
		if err := dir.EnsureDirectories(); err != nil {
			t.Fatal(err)
		}
	}

	indexes, err := r.ListIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(indexes))
	}
}
