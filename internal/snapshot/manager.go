package snapshot

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Manager tracks the current generation, distributes snapshots to readers,
// and manages segment reference counts for safe reclamation.
//
// Concurrency model:
//   - generationMu (RWMutex): Read-locked for snapshot acquisition,
//     write-locked for commit/manifest updates.
//   - snapshotsMu (Mutex): Protects the activeSnapshots map.
//   - Lock ordering: generationMu → snapshotsMu → SegmentRef.mu
//     Never acquire generationMu while holding snapshotsMu or SegmentRef.mu.
type Manager struct {
	// generationMu protects currentGeneration and currentSegments.
	generationMu sync.RWMutex

	currentGeneration uint64
	currentSegments   map[string]*SegmentRef // segmentID → ref

	// snapshotsMu protects activeSnapshots independently.
	snapshotsMu     sync.Mutex
	activeSnapshots map[uint64]*Snapshot // snapshotID → snapshot

	nextSnapshotID atomic.Uint64

	logger *slog.Logger

	// LeakThreshold is the duration after which a held snapshot is considered
	// a potential leak. Zero disables leak detection.
	LeakThreshold time.Duration
}

// NewManager creates a new SnapshotManager.
// initialGeneration is the recovered/current generation (0 for empty index).
// segments are the segment IDs from the current manifest.
func NewManager(initialGeneration uint64, segmentIDs []string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	refs := make(map[string]*SegmentRef, len(segmentIDs))
	for _, id := range segmentIDs {
		ref := NewSegmentRef(id)
		ref.SetInManifest(true)
		refs[id] = ref
	}

	return &Manager{
		currentGeneration: initialGeneration,
		currentSegments:   refs,
		activeSnapshots:   make(map[uint64]*Snapshot),
		logger:            logger,
		LeakThreshold:     5 * time.Minute,
	}
}

// Acquire creates a new Snapshot pinned to the current generation.
// The caller MUST call Snapshot.Release() when done.
func (m *Manager) Acquire() (*Snapshot, error) {
	m.generationMu.RLock()

	generation := m.currentGeneration
	var segments []*SegmentRef

	if generation != 0 {
		// Pin all current segments.
		segments = make([]*SegmentRef, 0, len(m.currentSegments))
		for _, ref := range m.currentSegments {
			ref.Pin()
			segments = append(segments, ref)
		}
	}

	m.generationMu.RUnlock()

	snap := &Snapshot{
		ID:         m.nextSnapshotID.Add(1),
		Generation: generation,
		AcquiredAt: time.Now(),
		Segments:   segments,
		manager:    m,
	}

	m.snapshotsMu.Lock()
	m.activeSnapshots[snap.ID] = snap
	m.snapshotsMu.Unlock()

	m.logger.Debug("snapshot acquired",
		"snapshot_id", snap.ID,
		"generation", snap.Generation,
		"segments", len(segments),
	)

	return snap, nil
}

// UpdateGeneration atomically updates the current generation and segment set.
// This is called after a successful commit or merge.
// Returns a list of segment IDs that are now reclaimable.
func (m *Manager) UpdateGeneration(newGeneration uint64, newSegmentIDs []string) []string {
	m.generationMu.Lock()
	defer m.generationMu.Unlock()

	if newGeneration <= m.currentGeneration {
		panic(fmt.Sprintf("snapshot: generation must be monotonically increasing: current=%d, new=%d",
			m.currentGeneration, newGeneration))
	}

	// Build new segment ref map.
	newRefs := make(map[string]*SegmentRef, len(newSegmentIDs))
	newSet := make(map[string]bool, len(newSegmentIDs))
	for _, id := range newSegmentIDs {
		newSet[id] = true
		if existing, ok := m.currentSegments[id]; ok {
			// Segment carried forward from previous generation.
			newRefs[id] = existing
		} else {
			// New segment.
			ref := NewSegmentRef(id)
			ref.SetInManifest(true)
			newRefs[id] = ref
		}
	}

	// Mark segments no longer in manifest.
	var reclaimable []string
	for id, ref := range m.currentSegments {
		if !newSet[id] {
			ref.SetInManifest(false)
			if ref.CanReclaim() {
				reclaimable = append(reclaimable, id)
			}
		}
	}

	m.currentGeneration = newGeneration
	m.currentSegments = newRefs

	m.logger.Info("generation updated",
		"generation", newGeneration,
		"segments", len(newSegmentIDs),
		"reclaimable", len(reclaimable),
	)

	return reclaimable
}

// CurrentGeneration returns the current committed generation.
func (m *Manager) CurrentGeneration() uint64 {
	m.generationMu.RLock()
	defer m.generationMu.RUnlock()
	return m.currentGeneration
}

// ActiveSnapshotCount returns the number of currently held snapshots.
func (m *Manager) ActiveSnapshotCount() int {
	m.snapshotsMu.Lock()
	defer m.snapshotsMu.Unlock()
	return len(m.activeSnapshots)
}

// SegmentRefCount returns the reference count for a segment, or -1 if unknown.
func (m *Manager) SegmentRefCount(segmentID string) int64 {
	m.generationMu.RLock()
	defer m.generationMu.RUnlock()
	if ref, ok := m.currentSegments[segmentID]; ok {
		return ref.RefCount()
	}
	return -1
}

// Reclaimable returns segment IDs that can be safely deleted.
func (m *Manager) Reclaimable() []string {
	m.generationMu.RLock()
	defer m.generationMu.RUnlock()

	var result []string
	for id, ref := range m.currentSegments {
		if ref.CanReclaim() {
			result = append(result, id)
		}
	}
	return result
}

// DetectLeaks returns snapshots that have been held longer than LeakThreshold.
func (m *Manager) DetectLeaks() []*Snapshot {
	if m.LeakThreshold <= 0 {
		return nil
	}

	m.snapshotsMu.Lock()
	defer m.snapshotsMu.Unlock()

	var leaks []*Snapshot
	for _, snap := range m.activeSnapshots {
		if snap.HeldDuration() > m.LeakThreshold {
			leaks = append(leaks, snap)
		}
	}
	return leaks
}

// releaseSnapshot removes a snapshot from the active set.
func (m *Manager) releaseSnapshot(snap *Snapshot) {
	m.snapshotsMu.Lock()
	delete(m.activeSnapshots, snap.ID)
	m.snapshotsMu.Unlock()

	m.logger.Debug("snapshot released",
		"snapshot_id", snap.ID,
		"generation", snap.Generation,
		"held_duration", snap.HeldDuration(),
	)
}
