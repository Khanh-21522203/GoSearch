package snapshot

import (
	"errors"
	"sync/atomic"
	"time"
)

var ErrSnapshotReleased = errors.New("snapshot already released")

// Snapshot represents a point-in-time view of a committed generation.
// It pins all segments in the generation so they cannot be reclaimed
// while the snapshot is active.
//
// Callers MUST call Release() when done. Failure to do so will leak
// segment references and prevent reclamation.
type Snapshot struct {
	// ID is a unique identifier for this snapshot.
	ID uint64

	// Generation is the committed generation this snapshot observes.
	Generation uint64

	// AcquiredAt is when this snapshot was acquired.
	AcquiredAt time.Time

	// Segments are the segment references pinned by this snapshot.
	Segments []*SegmentRef

	// manager is the SnapshotManager that created this snapshot.
	manager *Manager

	// released tracks whether Release has been called.
	released atomic.Bool
}

// Release unpins all segments and removes this snapshot from the manager.
// It is safe to call Release multiple times; subsequent calls are no-ops.
func (s *Snapshot) Release() error {
	if !s.released.CompareAndSwap(false, true) {
		return nil // Already released.
	}

	// Unpin all segments.
	for _, ref := range s.Segments {
		ref.Unpin()
	}

	// Notify manager.
	if s.manager != nil {
		s.manager.releaseSnapshot(s)
	}

	return nil
}

// Released returns true if this snapshot has been released.
func (s *Snapshot) Released() bool {
	return s.released.Load()
}

// HeldDuration returns how long this snapshot has been held.
func (s *Snapshot) HeldDuration() time.Duration {
	return time.Since(s.AcquiredAt)
}
