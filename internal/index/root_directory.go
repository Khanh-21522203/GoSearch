package index

import (
	"fmt"
	"path/filepath"

	"GoSearch/internal/storage"
)

// RootDir represents the top-level directory layout for the GoTextSearch server.
// All path methods are pure functions with no I/O side effects.
type RootDir struct {
	Root string
}

// NewRootDir creates a RootDir for the given root path.
func NewRootDir(root string) *RootDir {
	return &RootDir{Root: root}
}

// IndexesDir returns the path to the indexes/ directory.
func (r *RootDir) IndexesDir() string {
	return filepath.Join(r.Root, "indexes")
}

// GlobalDir returns the path to the global/ directory.
func (r *RootDir) GlobalDir() string {
	return filepath.Join(r.Root, "global")
}

// ConfigPath returns the path to global/config.json.
func (r *RootDir) ConfigPath() string {
	return filepath.Join(r.Root, "global", "config.json")
}

// LocksDir returns the path to global/locks/.
func (r *RootDir) LocksDir() string {
	return filepath.Join(r.Root, "global", "locks")
}

// LockPath returns the path to a specific index lock file.
func (r *RootDir) LockPath(indexName string) string {
	return filepath.Join(r.Root, "global", "locks", indexName+".lock")
}

// IndexDir returns an IndexDir for the given index name.
func (r *RootDir) IndexDir(indexName string) *IndexDir {
	return NewIndexDir(filepath.Join(r.Root, "indexes", indexName))
}

// EnsureDirectories creates all required top-level directories.
func (r *RootDir) EnsureDirectories() error {
	for _, dir := range []string{r.IndexesDir(), r.GlobalDir(), r.LocksDir()} {
		if err := storage.EnsureDir(dir); err != nil {
			return fmt.Errorf("ensure directory %s: %w", dir, err)
		}
	}
	return nil
}

// ListIndexes returns the names of all index directories.
func (r *RootDir) ListIndexes() ([]string, error) {
	return storage.ListSubdirs(r.IndexesDir())
}
