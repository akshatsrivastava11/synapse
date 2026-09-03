package storage

import (
	"akshat/synapse/internal/objectstore"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

type Partition struct {
	mu              sync.RWMutex
	id              int
	dir             string
	maxSegmentBytes int64
	store           objectstore.ObjectStore

	segments   []*Segment
	nextOffset int64
}

type PartitionObject struct {
	MaxSegmentBytes int64
	Store           objectstore.ObjectStore
}

var segmentFilePattern = regexp.MustCompile(`^(\d{20})\.log$`)

func OpenPartition(root string, id int, opts PartitionObject) (*Partition, error) {
	dir := filepath.Join(root, fmt.Sprintf("partition-%d", id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("open partition %d : %w", id, err)
	}
	starts, err := listSegmentStarts(dir)
	if err != nil {
		return nil, fmt.Errorf("open partition %d : %w", id, err)

	}
	p := &Partition{
		id:              id,
		dir:             dir,
		maxSegmentBytes: opts.MaxSegmentBytes,
		store:           opts.Store,
	}
	if len(starts) == 0 {
		seg, err := createSegment(dir, 0, opts.MaxSegmentBytes)
		if err != nil {
			return nil, err
		}
		p.segments = []*Segment{seg}
		p.nextOffset = 0
		return p, nil
	}
	for _, start := range starts {
		seg, err := openSegment(dir, start, opts.MaxSegmentBytes)
		if err != nil {
			return nil, fmt.Errorf("open partition %d : recovering segment %d: %w", id, start, err)
		}
		p.segments = append(p.segments, seg)

	}
	last := p.segments[len(p.segments)-1]
	p.nextOffset = last.startOffset + int64(last.recordCount())
	return p, nil

}

func listSegmentStarts(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var starts []int64
	for _, e := range entries {
		m := segmentFilePattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue
		}
		starts = append(starts, n)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	return starts, nil
}

func (p *Partition) Append(payload []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.segments[len(p.segments)-1]
	if active.shouldRoll() {
		newSeg, err := createSegment(p.dir, p.nextOffset, p.maxSegmentBytes)
		if err != nil {
			return 0, fmt.Errorf("partition %d: roll segment: %w", p.id, err)
		}
		active = newSeg
		p.segments = append(p.segments, active)
	}
	realIdx, err := active.append(payload)
	if err != nil {
		return 0, err
	}
	offset := active.startOffset + int64(realIdx)
	p.nextOffset = offset + 1
	return offset, nil
}

func (p *Partition) Sync() error {
	p.mu.RLock()
	active := p.segments[len(p.segments)-1]
	p.mu.RUnlock()
	return active.sync()
}

func (p *Partition) remoteKey(startOffset int64) string {
	return fmt.Sprintf("partition-%d/%s", p.id, segmentFileName(startOffset))
}

func (p *Partition) FlushSealedSegments(ctx context.Context) (flushedCount int, err error) {
	if p.store == nil {
		return 0, nil
	}
	p.mu.RLock()
	sealed := make([]*Segment, len(p.segments)-1)
	copy(sealed, p.segments[:len(p.segments)-1])
	p.mu.RUnlock()

	for _, seg := range sealed {
		key := p.remoteKey(seg.startOffset)
		uploaded, err := seg.flush(ctx, p.store, key)
		if err != nil {
			return flushedCount, fmt.Errorf("partition %d: flush segment start=%d: %w", p.id, seg.startOffset, err)
		}
		if uploaded {
			flushedCount++
		}
	}
	return flushedCount, nil
}
func (p *Partition) ReadFrom(ctx context.Context, offset int64) ([]byte, error) {
	p.mu.RLock()
	if offset < 0 || offset >= p.nextOffset {
		p.mu.RUnlock()
		return nil, fmt.Errorf("%w:offset %d,valid range [0,%d)", ErrOffsetOutOfRange, offset, p.nextOffset)
	}
	seg := p.findSegment(offset)
	p.mu.RUnlock()
	if seg == nil {
		return nil, fmt.Errorf("%w: offset %d", ErrOffsetOutOfRange, offset)
	}
	if p.store != nil {
		if err := seg.ensureLocal(ctx, p.store); err != nil {
			return nil, fmt.Errorf("partition %d: ensure local for read at offset %d: %w", p.id, offset, err)
		}
	}
	return seg.readAt(int(offset - seg.startOffset))
}

func (p *Partition) findSegment(offset int64) *Segment {
	i := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].startOffset > offset
	})
	if i == 0 {
		return nil
	}
	return p.segments[i-1]
}

func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}
func (p *Partition) SegmentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.segments)
}

func (p *Partition) LocalSegmentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, seg := range p.segments {
		if !seg.isRemote() {
			n++
		}
	}
	return n
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for _, seg := range p.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
