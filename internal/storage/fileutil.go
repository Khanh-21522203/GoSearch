package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// RemoveDirContents removes all entries inside a directory without removing
// the directory itself. Returns a list of removed paths for audit logging.
func RemoveDirContents(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	var removed []string
	var firstErr error
	for _, entry := range entries {
		p := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(p); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove %s: %w", p, err)
			}
			continue
		}
		removed = append(removed, p)
	}
	return removed, firstErr
}

// ListSubdirs returns the names (not full paths) of all immediate
// subdirectories within dir.
func ListSubdirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list subdirs %s: %w", dir, err)
	}

	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	return dirs, nil
}

// ListFiles returns the names (not full paths) of all regular files
// within dir (non-recursive).
func ListFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list files %s: %w", dir, err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

// FileExists returns true if the path exists and is a regular file.
func FileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// DirExists returns true if the path exists and is a directory.
func DirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
