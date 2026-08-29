package storage

import (
	"errors"
	"fmt"
	"testing"
)

func TestAppendAndReadInOrder(t *testing.T) {
	dir := t.TempDir()
	const maxSegmentBytes = 1024
	p, err := OpenPartition(dir, 0, maxSegmentBytes)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer p.Close()

	const numRecords = 10_000
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

	t.Logf("10k records produced %d segments", p.SegmentCount())
	for i := 0; i < numRecords; i++ {
		got, err := p.ReadFrom(int64(i))
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

func TestReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 0, 1024)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer p.Close()

	if _, err := p.Append([]byte("only record")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := p.ReadFrom(5); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("ReadFrom(5): got err %v, want ErrOffsetOutOfRange", err)
	}
	if _, err := p.ReadFrom(-1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("ReadFrom(-1): got err %v, want ErrOffsetOutOfRange", err)
	}
}
