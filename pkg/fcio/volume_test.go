package fcio

import (
	"context"
	"testing"
	"time"
)

// blocked asserts that no acquisition completes within the window.
func blocked(t *testing.T, what string, ch <-chan func()) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s granted while it must block", what)
	case <-time.After(120 * time.Millisecond):
	}
}

// granted asserts that an acquisition completes promptly.
func granted(t *testing.T, what string, ch <-chan func()) func() {
	t.Helper()
	select {
	case rel := <-ch:
		return rel
	case <-time.After(2 * time.Second):
		t.Fatalf("%s not granted", what)
		return nil
	}
}

func acquireReadAsync(ctx context.Context, vs *VolumeSet, id VolumeID, fs FsType) <-chan func() {
	ch := make(chan func(), 1)
	go func() {
		if rel, err := vs.AcquireRead(ctx, id, fs); err == nil {
			ch <- rel
		}
	}()
	return ch
}

func acquireRWAsync(ctx context.Context, vs *VolumeSet, read, write VolumeID, fs FsType) <-chan func() {
	ch := make(chan func(), 1)
	go func() {
		if rel, err := vs.AcquireRW(ctx, read, write, fs); err == nil {
			ch <- rel
		}
	}()
	return ch
}

func TestVolumeSetHDDReadVsRWExclusive(t *testing.T) {
	ctx := t.Context()
	vs := NewVolumeSet()
	relR, err := vs.AcquireRead(ctx, "vol1", FsHDD)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}
	// RW touching the held spindle must block until the reader releases.
	rwCh := acquireRWAsync(ctx, vs, "vol1", "vol2", FsHDD)
	blocked(t, "RW vs HDD reader", rwCh)
	relR()
	relRW := granted(t, "RW after reader release", rwCh)

	// While RW is held, a reader on the write volume must block.
	rCh := acquireReadAsync(ctx, vs, "vol2", FsHDD)
	blocked(t, "reader vs HDD RW", rCh)
	// And a second RW on the same spindle must block.
	rw2Ch := acquireRWAsync(ctx, vs, "vol2", "vol3", FsHDD)
	blocked(t, "second HDD RW", rw2Ch)
	relRW()
	// Both waiters are admitted once the RW releases; order is unspecified.
	for range 2 {
		select {
		case rel := <-rCh:
			rel()
		case rel := <-rw2Ch:
			rel()
		case <-time.After(2 * time.Second):
			t.Fatal("waiters not admitted after RW release")
		}
	}
}

func TestVolumeSetHDDReadersOverlap(t *testing.T) {
	ctx := t.Context()
	vs := NewVolumeSet()
	rel1, err := vs.AcquireRead(ctx, "vol1", FsHDD)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	// HDD readers overlap freely; a second reader must not block.
	rel2, err := vs.AcquireRead(ctx, "vol1", FsHDD)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	rel1()
	rel2()
}

func TestVolumeSetSSDOverlapRule(t *testing.T) {
	ctx := t.Context()
	vs := NewVolumeSet()
	// Two readers overlap.
	relR1, err := vs.AcquireRead(ctx, "vol1", FsSSD)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	relR2, err := vs.AcquireRead(ctx, "vol1", FsSSD)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	// A third reader and an RW both exceed the two-holder rule.
	r3Ch := acquireReadAsync(ctx, vs, "vol1", FsSSD)
	blocked(t, "third SSD reader", r3Ch)
	rwCh := acquireRWAsync(ctx, vs, "volX", "vol1", FsSSD)
	blocked(t, "SSD RW at two readers", rwCh)
	// Freeing one slot admits exactly one waiter (occupancy back to 2).
	relR2()
	var held func()
	select {
	case held = <-r3Ch:
		blocked(t, "SSD RW after third reader admitted", rwCh)
	case held = <-rwCh:
		blocked(t, "third SSD reader after RW admitted", r3Ch)
	case <-ctx.Done():
		t.Fatal("no waiter admitted after slot freed")
	}
	held()
	relR1()
	// The waiter that lost the earlier race now proceeds.
	select {
	case rel := <-r3Ch:
		rel()
	case rel := <-rwCh:
		rel()
	case <-time.After(2 * time.Second):
		t.Fatal("remaining SSD waiter not granted")
	}

	// 1 RW + 1 reader is legal.
	relRW, err := vs.AcquireRW(ctx, "volY", "vol2", FsSSD)
	if err != nil {
		t.Fatalf("rw: %v", err)
	}
	relR, err := vs.AcquireRead(ctx, "vol2", FsSSD)
	if err != nil {
		t.Fatalf("reader beside SSD RW: %v", err)
	}
	// A second RW on the same volume is not.
	rw2Ch := acquireRWAsync(ctx, vs, "volZ", "vol2", FsSSD)
	blocked(t, "second SSD RW", rw2Ch)
	relR()
	relRW()
	granted(t, "second SSD RW after release", rw2Ch)()
}

func TestVolumeSetAcquireContextCancel(t *testing.T) {
	vs := NewVolumeSet()
	rel, err := vs.AcquireRead(t.Context(), "vol1", FsHDD)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	defer rel()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := vs.AcquireRW(ctx, "vol1", "vol2", FsHDD); err == nil {
		t.Fatal("blocked AcquireRW must fail with cancelled context")
	}
}
