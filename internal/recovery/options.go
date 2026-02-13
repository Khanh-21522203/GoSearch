package recovery

import "log/slog"

// Options configures the crash recovery system.
type Options struct {
	// ManifestRetention is the number of old manifests to keep
	// in addition to the current one. Default: 2.
	ManifestRetention int

	// VerifySegmentChecksums controls whether segment file checksums
	// are verified during recovery. Default: true.
	VerifySegmentChecksums bool

	// Logger for recovery events. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		ManifestRetention:      2,
		VerifySegmentChecksums: true,
	}
}
