package snapshot

import (
	"sync"
	"testing"
	"time"
)

func TestManager_NewEmpty(t *testing.T) {
	m := NewManager(0, nil, nil)

	if m.CurrentGeneration() != 0 {
		t.Errorf("generation = %d, want 0", m.CurrentGeneration())
	}
	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
}

func TestManager_NewWithSegments(t *testing.T) {
	m := NewManager(5, []string{"seg_a", "seg_b"}, nil)

	if m.CurrentGeneration() != 5 {
		t.Errorf("generation = %d, want 5", m.CurrentGeneration())
	}
	if rc := m.SegmentRefCount("seg_a"); rc != 0 {
		t.Errorf("seg_a refcount = %d, want 0", rc)
	}
	if rc := m.SegmentRefCount("unknown"); rc != -1 {
		t.Errorf("unknown refcount = %d, want -1", rc)
	}
}

func TestManager_AcquireRelease_EmptyIndex(t *testing.T) {
	m := NewManager(0, nil, nil)

	snap, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Generation != 0 {
		t.Errorf("snapshot generation = %d, want 0", snap.Generation)
	}
	if len(snap.Segments) != 0 {
		t.Errorf("snapshot segments = %d, want 0", len(snap.Segments))
	}
	if m.ActiveSnapshotCount() != 1 {
		t.Errorf("active snapshots = %d, want 1", m.ActiveSnapshotCount())
	}

	if err := snap.Release(); err != nil {
		t.Fatal(err)
	}
	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots after release = %d, want 0", m.ActiveSnapshotCount())
	}
}

func TestManager_AcquireRelease_WithSegments(t *testing.T) {
	m := NewManager(3, []string{"seg_a", "seg_b"}, nil)

	snap, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Generation != 3 {
		t.Errorf("snapshot generation = %d, want 3", snap.Generation)
	}
	if len(snap.Segments) != 2 {
		t.Errorf("snapshot segments = %d, want 2", len(snap.Segments))
	}

	// Segments should be pinned.
	for _, ref := range snap.Segments {
		if ref.RefCount() != 1 {
			t.Errorf("segment %s refcount = %d, want 1", ref.SegmentID(), ref.RefCount())
		}
	}

	if err := snap.Release(); err != nil {
		t.Fatal(err)
	}

	// Segments should be unpinned.
	if rc := m.SegmentRefCount("seg_a"); rc != 0 {
		t.Errorf("seg_a refcount after release = %d, want 0", rc)
	}
}

func TestManager_MultipleSnapshots(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)

	s1, _ := m.Acquire()
	s2, _ := m.Acquire()

	if m.ActiveSnapshotCount() != 2 {
		t.Errorf("active snapshots = %d, want 2", m.ActiveSnapshotCount())
	}
	if rc := m.SegmentRefCount("seg_a"); rc != 2 {
		t.Errorf("seg_a refcount = %d, want 2", rc)
	}

	if err := s1.Release(); err != nil {
		t.Fatal(err)
	}
	if m.ActiveSnapshotCount() != 1 {
		t.Errorf("active snapshots = %d, want 1", m.ActiveSnapshotCount())
	}
	if rc := m.SegmentRefCount("seg_a"); rc != 1 {
		t.Errorf("seg_a refcount = %d, want 1", rc)
	}

	if err := s2.Release(); err != nil {
		t.Fatal(err)
	}
	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
}

func TestManager_DoubleRelease(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)

	snap, _ := m.Acquire()
	_ = snap.Release()
	_ = snap.Release() // Should be a no-op, not panic.

	if rc := m.SegmentRefCount("seg_a"); rc != 0 {
		t.Errorf("seg_a refcount = %d, want 0", rc)
	}
}

func TestManager_UpdateGeneration(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)

	// Commit adds seg_b, keeps seg_a.
	reclaimable := m.UpdateGeneration(2, []string{"seg_a", "seg_b"})
	if len(reclaimable) != 0 {
		t.Errorf("reclaimable = %d, want 0", len(reclaimable))
	}
	if m.CurrentGeneration() != 2 {
		t.Errorf("generation = %d, want 2", m.CurrentGeneration())
	}
}

func TestManager_UpdateGeneration_SegmentRemoved(t *testing.T) {
	m := NewManager(1, []string{"seg_a", "seg_b"}, nil)

	// Merge replaces seg_a + seg_b with seg_merged.
	reclaimable := m.UpdateGeneration(2, []string{"seg_merged"})

	// seg_a and seg_b should be reclaimable (refcount=0, not in manifest).
	if len(reclaimable) != 2 {
		t.Errorf("reclaimable = %d, want 2", len(reclaimable))
	}
}

func TestManager_UpdateGeneration_PinnedSegmentNotReclaimable(t *testing.T) {
	m := NewManager(1, []string{"seg_a", "seg_b"}, nil)

	// Reader pins seg_a and seg_b.
	snap, _ := m.Acquire()

	// Merge replaces both with seg_merged.
	reclaimable := m.UpdateGeneration(2, []string{"seg_merged"})

	// seg_a and seg_b are still pinned by the snapshot → not reclaimable.
	if len(reclaimable) != 0 {
		t.Errorf("reclaimable = %d, want 0 (segments still pinned)", len(reclaimable))
	}

	// After release, they become reclaimable (but we'd need to check again).
	if err := snap.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestManager_UpdateGeneration_PanicsOnNonMonotonic(t *testing.T) {
	m := NewManager(5, nil, nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on non-monotonic generation")
		}
	}()

	m.UpdateGeneration(3, nil) // Should panic.
}

func TestManager_CommitDuringActiveRead(t *testing.T) {
	m := NewManager(5, []string{"seg_a", "seg_b"}, nil)

	// Reader 1 acquires snapshot at gen 5.
	reader1, _ := m.Acquire()
	if reader1.Generation != 5 {
		t.Errorf("reader1 generation = %d, want 5", reader1.Generation)
	}

	// Writer commits gen 6 with new segment.
	m.UpdateGeneration(6, []string{"seg_a", "seg_b", "seg_c"})

	// Reader 2 acquires snapshot at gen 6.
	reader2, _ := m.Acquire()
	if reader2.Generation != 6 {
		t.Errorf("reader2 generation = %d, want 6", reader2.Generation)
	}
	if len(reader2.Segments) != 3 {
		t.Errorf("reader2 segments = %d, want 3", len(reader2.Segments))
	}

	// Reader 1 still sees gen 5 with 2 segments.
	if reader1.Generation != 5 {
		t.Errorf("reader1 generation changed to %d, want 5", reader1.Generation)
	}
	if len(reader1.Segments) != 2 {
		t.Errorf("reader1 segments = %d, want 2", len(reader1.Segments))
	}

	if err := reader1.Release(); err != nil {
		t.Fatal(err)
	}
	if err := reader2.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestManager_MergeDuringActiveRead(t *testing.T) {
	m := NewManager(10, []string{"seg_a", "seg_b", "seg_c"}, nil)

	// Reader pins gen 10.
	reader, _ := m.Acquire()

	// Merge: seg_a + seg_b → seg_merged, keep seg_c.
	reclaimable := m.UpdateGeneration(11, []string{"seg_merged", "seg_c"})

	// seg_a and seg_b still pinned by reader.
	if len(reclaimable) != 0 {
		t.Errorf("reclaimable = %d, want 0", len(reclaimable))
	}

	// Reader releases.
	if err := reader.Release(); err != nil {
		t.Fatal(err)
	}

	// Now seg_a and seg_b should have refcount 0.
	// (They are tracked in the old currentSegments, not the new one,
	// so we can't query them via SegmentRefCount anymore.
	// The reclaimable list was empty at UpdateGeneration time.)
}

func TestManager_ConcurrentAcquireRelease(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap, err := m.Acquire()
			if err != nil {
				t.Errorf("acquire error: %v", err)
				return
			}
			// Simulate some work.
			time.Sleep(time.Microsecond)
			_ = snap.Release()
		}()
	}
	wg.Wait()

	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
	if rc := m.SegmentRefCount("seg_a"); rc != 0 {
		t.Errorf("seg_a refcount = %d, want 0", rc)
	}
}

func TestManager_DetectLeaks(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)
	m.LeakThreshold = 1 * time.Millisecond

	snap, _ := m.Acquire()
	time.Sleep(5 * time.Millisecond)

	leaks := m.DetectLeaks()
	if len(leaks) != 1 {
		t.Errorf("leaks = %d, want 1", len(leaks))
	}

	if err := snap.Release(); err != nil {
		t.Fatal(err)
	}

	leaks = m.DetectLeaks()
	if len(leaks) != 0 {
		t.Errorf("leaks after release = %d, want 0", len(leaks))
	}
}

func TestManager_DetectLeaks_Disabled(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)
	m.LeakThreshold = 0

	snap, _ := m.Acquire()
	defer func() { _ = snap.Release() }()

	leaks := m.DetectLeaks()
	if len(leaks) != 0 {
		t.Errorf("leaks = %d, want 0 (detection disabled)", len(leaks))
	}
}

func TestSnapshot_Released(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)
	snap, _ := m.Acquire()

	if snap.Released() {
		t.Error("should not be released yet")
	}

	_ = snap.Release()
	if !snap.Released() {
		t.Error("should be released")
	}
}

func TestSnapshot_HeldDuration(t *testing.T) {
	m := NewManager(1, []string{"seg_a"}, nil)
	snap, _ := m.Acquire()
	time.Sleep(2 * time.Millisecond)

	d := snap.HeldDuration()
	if d < time.Millisecond {
		t.Errorf("held duration = %v, expected >= 1ms", d)
	}

	_ = snap.Release()
}
