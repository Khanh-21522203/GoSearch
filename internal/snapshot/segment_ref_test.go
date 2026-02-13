package snapshot

import (
	"testing"
)

func TestSegmentRef_PinUnpin(t *testing.T) {
	ref := NewSegmentRef("seg_1")

	if ref.RefCount() != 0 {
		t.Errorf("initial refcount = %d, want 0", ref.RefCount())
	}

	ref.Pin()
	if ref.RefCount() != 1 {
		t.Errorf("after pin refcount = %d, want 1", ref.RefCount())
	}

	ref.Pin()
	if ref.RefCount() != 2 {
		t.Errorf("after second pin refcount = %d, want 2", ref.RefCount())
	}

	ref.Unpin()
	if ref.RefCount() != 1 {
		t.Errorf("after unpin refcount = %d, want 1", ref.RefCount())
	}

	ref.Unpin()
	if ref.RefCount() != 0 {
		t.Errorf("after second unpin refcount = %d, want 0", ref.RefCount())
	}
}

func TestSegmentRef_SegmentID(t *testing.T) {
	ref := NewSegmentRef("seg_gen_42_abcd")
	if ref.SegmentID() != "seg_gen_42_abcd" {
		t.Errorf("SegmentID = %s, want seg_gen_42_abcd", ref.SegmentID())
	}
}

func TestSegmentRef_CanReclaim(t *testing.T) {
	ref := NewSegmentRef("seg_1")

	// Initially: refCount=0, inManifest=false → reclaimable.
	if !ref.CanReclaim() {
		t.Error("should be reclaimable when refCount=0 and not in manifest")
	}

	// In manifest → not reclaimable.
	ref.SetInManifest(true)
	if ref.CanReclaim() {
		t.Error("should not be reclaimable when in manifest")
	}

	// Pinned + in manifest → not reclaimable.
	ref.Pin()
	if ref.CanReclaim() {
		t.Error("should not be reclaimable when pinned and in manifest")
	}

	// Pinned + not in manifest → not reclaimable.
	ref.SetInManifest(false)
	if ref.CanReclaim() {
		t.Error("should not be reclaimable when pinned")
	}

	// Unpinned + not in manifest → reclaimable.
	ref.Unpin()
	if !ref.CanReclaim() {
		t.Error("should be reclaimable when refCount=0 and not in manifest")
	}
}

func TestSegmentRef_InManifest(t *testing.T) {
	ref := NewSegmentRef("seg_1")

	if ref.InManifest() {
		t.Error("should not be in manifest initially")
	}

	ref.SetInManifest(true)
	if !ref.InManifest() {
		t.Error("should be in manifest after SetInManifest(true)")
	}

	ref.SetInManifest(false)
	if ref.InManifest() {
		t.Error("should not be in manifest after SetInManifest(false)")
	}
}

func TestSegmentRef_UnpinPanicsOnNegative(t *testing.T) {
	ref := NewSegmentRef("seg_1")

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on negative refcount")
		}
	}()

	ref.Unpin() // Should panic.
}
