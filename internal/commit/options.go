package commit

import "log/slog"

// Options configures the commit protocol.
type Options struct {
	// SchemaVersion is the current schema version to embed in manifests.
	SchemaVersion uint32

	// Logger for commit protocol events. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		SchemaVersion: 1,
	}
}
