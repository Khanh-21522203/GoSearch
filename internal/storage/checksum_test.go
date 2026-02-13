package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestComputeChecksum(t *testing.T) {
	// Known SHA-256 vector: sha256("hello") = 2cf24dba...
	data := []byte("hello")
	expected := sha256.Sum256(data)
	expectedStr := ChecksumPrefix + hex.EncodeToString(expected[:])

	got := ComputeChecksum(data)
	if string(got) != expectedStr {
		t.Errorf("ComputeChecksum(%q) = %s, want %s", data, got, expectedStr)
	}
}

func TestComputeChecksum_Empty(t *testing.T) {
	// SHA-256 of empty input
	expected := sha256.Sum256(nil)
	expectedStr := ChecksumPrefix + hex.EncodeToString(expected[:])

	got := ComputeChecksum(nil)
	if string(got) != expectedStr {
		t.Errorf("ComputeChecksum(nil) = %s, want %s", got, expectedStr)
	}
}

func TestComputeFileChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")
	data := []byte("test file content for checksum")

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ComputeFileChecksum(path)
	if err != nil {
		t.Fatal(err)
	}

	expected := ComputeChecksum(data)
	if got != expected {
		t.Errorf("ComputeFileChecksum = %s, want %s", got, expected)
	}
}

func TestComputeFileChecksum_NotExists(t *testing.T) {
	_, err := ComputeFileChecksum("/nonexistent/path/file")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestVerifyFileChecksum_Match(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")
	data := []byte("verify me")

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	checksum := ComputeChecksum(data)
	if err := VerifyFileChecksum(path, checksum); err != nil {
		t.Errorf("VerifyFileChecksum should succeed: %v", err)
	}
}

func TestVerifyFileChecksum_Mismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")

	if err := os.WriteFile(path, []byte("actual content"), 0644); err != nil {
		t.Fatal(err)
	}

	wrongChecksum := ComputeChecksum([]byte("different content"))
	err := VerifyFileChecksum(path, wrongChecksum)
	if err == nil {
		t.Error("expected checksum mismatch error")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got: %v", err)
	}
}

func TestFormatChecksum(t *testing.T) {
	raw := sha256.Sum256([]byte("test"))
	c := FormatChecksum(raw[:])

	if len(c) != len(ChecksumPrefix)+64 {
		t.Errorf("unexpected checksum length: %d", len(c))
	}
	if string(c[:len(ChecksumPrefix)]) != ChecksumPrefix {
		t.Errorf("checksum missing prefix: %s", c)
	}
}

func TestParseChecksum_Valid(t *testing.T) {
	data := []byte("parse test")
	c := ComputeChecksum(data)

	hexStr, err := ParseChecksum(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(hexStr) != 64 {
		t.Errorf("expected 64 hex chars, got %d", len(hexStr))
	}
}

func TestParseChecksum_MissingPrefix(t *testing.T) {
	_, err := ParseChecksum(Checksum("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"))
	if err == nil {
		t.Error("expected error for missing prefix")
	}
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Errorf("expected ErrInvalidChecksum, got: %v", err)
	}
}

func TestParseChecksum_WrongLength(t *testing.T) {
	_, err := ParseChecksum(Checksum(ChecksumPrefix + "tooshort"))
	if err == nil {
		t.Error("expected error for wrong length")
	}
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Errorf("expected ErrInvalidChecksum, got: %v", err)
	}
}

func TestParseChecksum_InvalidHex(t *testing.T) {
	_, err := ParseChecksum(Checksum(ChecksumPrefix + "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"))
	if err == nil {
		t.Error("expected error for invalid hex")
	}
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Errorf("expected ErrInvalidChecksum, got: %v", err)
	}
}

func TestComputeReaderChecksum(t *testing.T) {
	data := []byte("reader checksum test")
	r := newBytesReader(data)

	got, err := ComputeReaderChecksum(r, nil)
	if err != nil {
		t.Fatal(err)
	}

	expected := ComputeChecksum(data)
	if got != expected {
		t.Errorf("ComputeReaderChecksum = %s, want %s", got, expected)
	}
}

// bytesReader wraps bytes for io.Reader.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}
