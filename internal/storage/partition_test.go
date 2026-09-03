package storage

import (
	"akshat/synapse/internal/objectstore"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestAppendAndReadInOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewMemoryStore()
	const maxSegmentBytes = 1024
	p, err := OpenPartition(dir, 0, PartitionObject{
		MaxSegmentBytes: maxSegmentBytes,
		Store:           store,
	})
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer p.Close()

	const numRecords = 1000
	for i := 0; i < numRecords; i++ {
		payload := []byte(fmt.Sprintf("record-%d", i))
		offset, err := p.Append(payload)
		if err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("Append(%d): got offset %d, want %d (offsets must be gapless and monotonic)", i, offset, i)
		}
	}
	if err := p.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if got := p.SegmentCount(); got < 2 {
		t.Fatalf("expected multiple segment rolls with maxSegmentBytes=%d, got only %d segment(s)", maxSegmentBytes, got)
	}

	t.Logf("1k records produced %d segments", p.SegmentCount())
	for i := 0; i < numRecords; i++ {
		got, err := p.ReadFrom(ctx, int64(i))
		if err != nil {
			t.Fatalf("ReadFrom(%d): %v", i, err)
		}
		want := fmt.Sprintf("record-%d", i)
		if string(got) != want {
			t.Fatalf("ReadFrom(%d): got %q, want %q", i, got, want)
		}
	}

	if got := p.NextOffset(); got != numRecords {
		t.Fatalf("NextOffset: got %d, want %d", got, numRecords)
	}
}

func TestFlushAndTieredReads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewMemoryStore()
	p, err := OpenPartition(dir, 0, PartitionObject{
		MaxSegmentBytes: 200,
		Store:           store,
	})
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer p.Close()

	const numRecords = 60
	for i := 0; i < numRecords; i++ {
		payload := []byte(fmt.Sprintf("event-%03d", i))
		if _, err := p.Append(payload); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	if p.LocalSegmentCount() != p.SegmentCount() {
		t.Fatalf("expected all segments local before flush")
	}

	flushed, err := p.FlushSealedSegments(ctx)
	if err != nil {
		t.Fatalf("FlushSealedSegments: %v", err)
	}
	if flushed == 0 {
		t.Fatalf("expected at least 1 flushed segment")
	}
	if p.LocalSegmentCount() != 1 {
		t.Fatalf("expected only 1 local active segment after flush, got %d", p.LocalSegmentCount())
	}

	// Cold read from offset 0
	val, err := p.ReadFrom(ctx, 0)
	if err != nil {
		t.Fatalf("Cold ReadFrom(0): %v", err)
	}
	if string(val) != "event-000" {
		t.Fatalf("Cold ReadFrom(0): got %s, want event-000", val)
	}
	if p.LocalSegmentCount() != 2 {
		t.Fatalf("expected 2 local segments after cold read re-cache, got %d", p.LocalSegmentCount())
	}
}

func TestReadOutOfRange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p, err := OpenPartition(dir, 0, PartitionObject{
		MaxSegmentBytes: 1024,
	})
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer p.Close()

	if _, err := p.Append([]byte("only record")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := p.ReadFrom(ctx, 5); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("ReadFrom(5): got err %v, want ErrOffsetOutOfRange", err)
	}
	if _, err := p.ReadFrom(ctx, -1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("ReadFrom(-1): got err %v, want ErrOffsetOutOfRange", err)
	}
}
