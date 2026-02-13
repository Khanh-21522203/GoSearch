package integration

import (
	"sync"
	"testing"
	"time"

	"GoSearch/internal/snapshot"
)

func TestConcurrentReaders(t *testing.T) {
	m := snapshot.NewManager(1, []string{"seg_a", "seg_b"}, nil)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Spawn 50 concurrent readers.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap, err := m.Acquire()
			if err != nil {
				errors <- err
				return
			}
			defer snap.Release()

			// Simulate query work.
			if snap.Generation != 1 {
				t.Errorf("expected generation 1, got %d", snap.Generation)
			}
			if len(snap.Segments) != 2 {
				t.Errorf("expected 2 segments, got %d", len(snap.Segments))
			}
			time.Sleep(time.Microsecond)
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("reader error: %v", err)
	}

	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
}

func TestConcurrentReadersWithCommit(t *testing.T) {
	m := snapshot.NewManager(1, []string{"seg_a"}, nil)

	// Reader 1 acquires snapshot at gen 1.
	reader1, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	// Spawn readers on gen 1.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap, err := m.Acquire()
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			snap.Release()
		}()
	}

	// Commit gen 2 while readers are active.
	time.Sleep(500 * time.Microsecond)
	m.UpdateGeneration(2, []string{"seg_a", "seg_b"})

	// Reader 1 should still see gen 1.
	if reader1.Generation != 1 {
		t.Errorf("reader1 generation = %d, want 1", reader1.Generation)
	}
	if len(reader1.Segments) != 1 {
		t.Errorf("reader1 segments = %d, want 1", len(reader1.Segments))
	}

	// New readers should see gen 2.
	reader2, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if reader2.Generation != 2 {
		t.Errorf("reader2 generation = %d, want 2", reader2.Generation)
	}
	if len(reader2.Segments) != 2 {
		t.Errorf("reader2 segments = %d, want 2", len(reader2.Segments))
	}
	reader2.Release()

	reader1.Release()
	wg.Wait()

	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
}

func TestConcurrentReadersWithMerge(t *testing.T) {
	m := snapshot.NewManager(5, []string{"seg_a", "seg_b", "seg_c"}, nil)

	// Acquire snapshots before merge.
	snaps := make([]*snapshot.Snapshot, 10)
	for i := range snaps {
		var err error
		snaps[i], err = m.Acquire()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Merge: seg_a + seg_b → seg_merged, keep seg_c.
	reclaimable := m.UpdateGeneration(6, []string{"seg_merged", "seg_c"})

	// Old segments still pinned → not reclaimable.
	if len(reclaimable) != 0 {
		t.Errorf("reclaimable = %d, want 0 (segments still pinned)", len(reclaimable))
	}

	// All readers still see gen 5 with 3 segments.
	for i, snap := range snaps {
		if snap.Generation != 5 {
			t.Errorf("snap[%d] generation = %d, want 5", i, snap.Generation)
		}
		if len(snap.Segments) != 3 {
			t.Errorf("snap[%d] segments = %d, want 3", i, len(snap.Segments))
		}
	}

	// Release all.
	for _, snap := range snaps {
		snap.Release()
	}

	if m.ActiveSnapshotCount() != 0 {
		t.Errorf("active snapshots = %d, want 0", m.ActiveSnapshotCount())
	}
}
