package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	// ChecksumPrefix is the prefix for SHA-256 checksums.
	ChecksumPrefix = "sha256:"

	// checksumBufSize is the buffer size for streaming checksum computation.
	checksumBufSize = 32 * 1024 // 32KB
)

// Checksum represents a hex-encoded SHA-256 hash with the "sha256:" prefix.
type Checksum string

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrInvalidChecksum  = errors.New("invalid checksum format")
)

// bufPool pools 32KB buffers for streaming checksum computation.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, checksumBufSize)
		return &buf
	},
}

// ComputeChecksum computes SHA-256 over a byte slice.
func ComputeChecksum(data []byte) Checksum {
	sum := sha256.Sum256(data)
	return FormatChecksum(sum[:])
}

// ComputeFileChecksum opens a file and computes its SHA-256 checksum.
func ComputeFileChecksum(path string) (Checksum, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("compute file checksum %s: %w", path, err)
	}
	defer f.Close()

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)

	return ComputeReaderChecksum(f, *bufPtr)
}

// ComputeReaderChecksum computes SHA-256 by streaming from an io.Reader.
// If buf is nil, a default 32KB buffer is allocated.
func ComputeReaderChecksum(r io.Reader, buf []byte) (Checksum, error) {
	h := sha256.New()
	if buf == nil {
		buf = make([]byte, checksumBufSize)
	}
	if _, err := io.CopyBuffer(h, r, buf); err != nil {
		return "", fmt.Errorf("compute reader checksum: %w", err)
	}
	return FormatChecksum(h.Sum(nil)), nil
}

// VerifyFileChecksum verifies that a file's SHA-256 matches the expected checksum.
func VerifyFileChecksum(path string, expected Checksum) error {
	actual, err := ComputeFileChecksum(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: file %s expected %s got %s", ErrChecksumMismatch, path, expected, actual)
	}
	return nil
}

// FormatChecksum formats raw hash bytes into a Checksum with the "sha256:" prefix.
func FormatChecksum(sum []byte) Checksum {
	return Checksum(ChecksumPrefix + hex.EncodeToString(sum))
}

// ParseChecksum strips the "sha256:" prefix and returns the raw hex string.
func ParseChecksum(c Checksum) (string, error) {
	s := string(c)
	if !strings.HasPrefix(s, ChecksumPrefix) {
		return "", fmt.Errorf("%w: missing prefix %q", ErrInvalidChecksum, ChecksumPrefix)
	}
	hexStr := s[len(ChecksumPrefix):]
	if len(hexStr) != 64 {
		return "", fmt.Errorf("%w: expected 64 hex chars, got %d", ErrInvalidChecksum, len(hexStr))
	}
	// Validate hex
	if _, err := hex.DecodeString(hexStr); err != nil {
		return "", fmt.Errorf("%w: invalid hex: %v", ErrInvalidChecksum, err)
	}
	return hexStr, nil
}
