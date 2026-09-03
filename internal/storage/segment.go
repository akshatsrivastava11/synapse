package storage

import (
	"akshat/synapse/internal/objectstore"
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Segment struct {
	mu sync.RWMutex

	dir         string
	startOffset int64
	maxBytes    int64

	file        *os.File
	writePos    int64   // current EOF byte position
	filePos     []int64 // filePos[i] = byte offset of record startOffset + i
	sizeBytes   int64

	remote      bool
	remoteKey   string
	frozenCount int
}

func segmentFileName(startOffset int64) string {
	return fmt.Sprintf("%020d.log", startOffset)
}

func createSegment(dir string, startOffset int64, maxBytes int64) (*Segment, error) {
	path := filepath.Join(dir, segmentFileName(startOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("create segment %s: %w", path, err)
	}
	return &Segment{
		dir:         dir,
		startOffset: startOffset,
		file:        f,
		maxBytes:    maxBytes,
	}, nil
}

func openSegment(dir string, startOffset int64, maxBytes int64) (*Segment, error) {
	path := filepath.Join(dir, segmentFileName(startOffset))
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	s := &Segment{
		dir:         dir,
		startOffset: startOffset,
		file:        f,
		maxBytes:    maxBytes,
	}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func scanFrames(r io.Reader) (position []int64, endPos int64, err error) {
	br := bufio.NewReader(r)
	var pos int64
	for {
		start := pos
		payload, ferr := readFrame(br)
		if ferr == io.EOF {
			return position, pos, nil
		}
		if ferr != nil {
			return position, start, ferr
		}
		position = append(position, start)
		pos += int64(headerBytes + len(payload))
	}
}

func (s *Segment) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	position, endPos, err := scanFrames(s.file)
	if err == io.ErrUnexpectedEOF || err == ErrCorruptRecord {
		if truncerr := s.file.Truncate(endPos); truncerr != nil {
			return fmt.Errorf("recover segment: truncate after bad frame: %w", truncerr)
		}
	} else if err != nil {
		return fmt.Errorf("recover segment: %w", err)
	}
	s.filePos = position
	s.sizeBytes = endPos
	s.writePos = endPos
	if _, err := s.file.Seek(endPos, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (s *Segment) rebuildIndexLocked(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	positions, endPos, err := scanFrames(f)
	if err != nil {
		return fmt.Errorf("downloaded segment data is corrupt or truncated: %w", err)
	}
	s.filePos = positions
	s.writePos = endPos
	s.sizeBytes = endPos
	if _, err := f.Seek(endPos, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (s *Segment) append(payload []byte) (relativeIndex int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remote || s.file == nil {
		return 0, fmt.Errorf("cannot append to remote or closed segment start=%d", s.startOffset)
	}
	frame := EncodeFrame(payload)
	pos := s.writePos
	if _, err := s.file.Write(frame); err != nil {
		return 0, fmt.Errorf("segment append: %w", err)
	}
	s.filePos = append(s.filePos, pos)
	s.writePos += int64(len(frame))
	s.sizeBytes += int64(len(frame))
	return len(s.filePos) - 1, err
}

func (s *Segment) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	return s.file.Sync()
}

func (s *Segment) readAt(relativeIndex int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return nil, fmt.Errorf("segment file is not resident locally")
	}
	if relativeIndex < 0 || relativeIndex >= len(s.filePos) {
		return nil, fmt.Errorf("%w: relative index %d out of range [0,%d)", ErrOffsetOutOfRange, relativeIndex, len(s.filePos))
	}
	pos := s.filePos[relativeIndex]
	header := make([]byte, headerBytes)
	if _, err := s.file.ReadAt(header, pos); err != nil {
		return nil, fmt.Errorf("segment read header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[0:4])
	wantCRC := binary.BigEndian.Uint32(header[4:8])
	payload := make([]byte, length)
	if _, err := s.file.ReadAt(payload, pos+headerBytes); err != nil {
		return nil, fmt.Errorf("segment read payload: %w", err)
	}
	if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
		return nil, ErrCorruptRecord
	}
	return payload, nil
}

func (s *Segment) ensureLocal(ctx context.Context, store objectstore.ObjectStore) error {
	s.mu.Lock()
	if s.file != nil {
		s.mu.Unlock()
		return nil
	}
	if !s.remote {
		s.mu.Unlock()
		return fmt.Errorf("segment start=%d has no local file and is not marked remote", s.startOffset)
	}
	key := s.remoteKey
	s.mu.Unlock()

	data, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("download segment start=%d key=%s: %w", s.startOffset, key, err)
	}
	path := filepath.Join(s.dir, segmentFileName(s.startOffset))
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("cache segment start=%d to disk: %w", s.startOffset, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open cached segment start=%d: %w", s.startOffset, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		f.Close()
		return nil
	}
	if err := s.rebuildIndexLocked(f); err != nil {
		f.Close()
		return fmt.Errorf("rebuild index for cached segment start=%d: %w", s.startOffset, err)
	}
	s.file = f
	s.remote = false
	return nil
}

func (s *Segment) flush(ctx context.Context, store objectstore.ObjectStore, key string) (didUpload bool, err error) {
	s.mu.Lock()
	if s.remote {
		s.mu.Unlock()
		return false, nil
	}
	if s.file == nil {
		s.mu.Unlock()
		return false, fmt.Errorf("cannot flush segment start=%d: no local file resident", s.startOffset)
	}
	size := s.sizeBytes
	data := make([]byte, size)
	if _, err := s.file.ReadAt(data, 0); err != nil && err != io.EOF {
		s.mu.Unlock()
		return false, fmt.Errorf("read segment start=%d for flush: %w", s.startOffset, err)
	}
	alreadyUploaded := (s.remoteKey == key)
	s.mu.Unlock()

	if !alreadyUploaded {
		if err := store.Put(ctx, key, data); err != nil {
			return false, fmt.Errorf("upload segment start=%d key=%s: %w", s.startOffset, key, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remote || s.file == nil {
		return !alreadyUploaded, nil
	}
	path := s.file.Name()
	s.frozenCount = len(s.filePos)
	if err := s.file.Close(); err != nil {
		return false, fmt.Errorf("close local segment start=%d after flush: %w", s.startOffset, err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("remove local segment start=%d after flush: %w", s.startOffset, err)
	}
	s.file = nil
	s.filePos = nil
	s.remote = true
	s.remoteKey = key
	return !alreadyUploaded, nil
}

func (s *Segment) recordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return s.frozenCount
	}
	return len(s.filePos)
}

func (s *Segment) isRemote() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.remote
}

func (s *Segment) shouldRoll() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes >= s.maxBytes
}

func (s *Segment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
