package snapshot

import (
	"sync"
	"sync/atomic"
)

// SegmentRef tracks the reference count for a single segment.
// It is safe for concurrent use.
type SegmentRef struct {
	segmentID string
	refCount  atomic.Int64
	mu        sync.Mutex // protects reclaim-related checks
	inManifest bool      // true if referenced by current manifest
}

// NewSegmentRef creates a new SegmentRef with refCount 0.
func NewSegmentRef(segmentID string) *SegmentRef {
	return &SegmentRef{
		segmentID: segmentID,
	}
}

// SegmentID returns the segment's identifier.
func (r *SegmentRef) SegmentID() string {
	return r.segmentID
}

// Pin increments the reference count. Called when a snapshot acquires this segment.
func (r *SegmentRef) Pin() {
	r.refCount.Add(1)
}

// Unpin decrements the reference count. Called when a snapshot releases this segment.
func (r *SegmentRef) Unpin() {
	newVal := r.refCount.Add(-1)
	if newVal < 0 {
		panic("snapshot: segment ref count went negative for " + r.segmentID)
	}
}

// RefCount returns the current reference count.
func (r *SegmentRef) RefCount() int64 {
	return r.refCount.Load()
}

// SetInManifest marks whether this segment is referenced by the current manifest.
func (r *SegmentRef) SetInManifest(inManifest bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inManifest = inManifest
}

// InManifest returns whether this segment is in the current manifest.
func (r *SegmentRef) InManifest() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inManifest
}

// CanReclaim returns true if the segment can be safely deleted.
// A segment is reclaimable when its reference count is zero AND it is not
// referenced by the current manifest.
func (r *SegmentRef) CanReclaim() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refCount.Load() == 0 && !r.inManifest
}
