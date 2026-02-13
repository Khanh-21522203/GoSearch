package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DirPerm  os.FileMode = 0755
	FilePerm os.FileMode = 0644
	LockPerm os.FileMode = 0600
)

// FsyncFile opens the file at path and calls fsync on it.
func FsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("fsync file open %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync file sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("fsync file close %s: %w", path, err)
	}
	return nil
}

// FsyncDir opens the directory at path and calls fsync on it.
// This ensures directory entries (file names) are durable.
func FsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("fsync dir open %s: %w", path, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("fsync dir sync %s: %w", path, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("fsync dir close %s: %w", path, err)
	}
	return nil
}

// AtomicWriteFile writes data to a temporary file in tmpDir, fsyncs it,
// then renames it to finalPath, and fsyncs the parent directory of finalPath.
// tmpDir must be on the same filesystem as finalPath.
func AtomicWriteFile(finalPath string, data []byte, tmpDir string) error {
	tmp, err := os.CreateTemp(tmpDir, "atomic-*")
	if err != nil {
		return fmt.Errorf("atomic write create temp in %s: %w", tmpDir, err)
	}
	tmpPath := tmp.Name()

	// Clean up temp file on any error.
	success := false
	defer func() {
		if !success {
			os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("atomic write data: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("atomic write fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("atomic write close: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("atomic write rename %s → %s: %w", tmpPath, finalPath, err)
	}
	if err := FsyncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("atomic write fsync parent dir: %w", err)
	}

	success = true
	return nil
}

// AtomicWriteFileFromTemp renames an already-written and fsynced file from
// tmpPath to finalPath, then fsyncs the parent directory of finalPath.
func AtomicWriteFileFromTemp(tmpPath, finalPath string) error {
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename %s → %s: %w", tmpPath, finalPath, err)
	}
	if err := FsyncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("fsync parent dir of %s: %w", finalPath, err)
	}
	return nil
}

// WriteFileSync writes data to path with the given permissions, fsyncs the file,
// and closes it. Does NOT fsync the parent directory.
func WriteFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("write file open %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write file data %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("write file sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write file close %s: %w", path, err)
	}
	return nil
}

// EnsureDir creates a directory (and parents) if it does not exist.
func EnsureDir(path string) error {
	return os.MkdirAll(path, DirPerm)
}
