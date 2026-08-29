package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Segment struct {
	mu          sync.RWMutex
	dir         string
	startOffset int64
	file        *os.File
	writePos    int64   //curent EOF byte position
	filePos     []int64 //filePos[i]=byte offset of the record startOffset + i
	sizeBytes   int64
	maxBytes    int64
}

func segmentFileName(startOffset int64) string {
	return fmt.Sprintf("%020d.log", startOffset)
}

func createSegment(dir string, startOffset int64, maxBytes int64) (*Segment, error) {
	path := filepath.Join(dir, segmentFileName(startOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("create segment %s:%w", path, err)
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
		return nil, fmt.Errorf("open segment  %s : %w", path, err)
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

func (s *Segment) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.file)
	var pos int64

	for {
		startOffFrame := pos
		payload, err := readFrame(r)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || err == ErrCorruptRecord {
			if truncErr := s.file.Truncate(startOffFrame); truncErr != nil {
				return fmt.Errorf("recover segment : truncate after bad frame: %w", truncErr)
			}
			pos = startOffFrame
			break
		}
		if err != nil {
			return fmt.Errorf("recover segment: %w", err)
		}
		s.filePos = append(s.filePos, startOffFrame)
		frameLen := int64(headerBytes + len(payload))
		pos += frameLen
	}
	s.writePos = pos
	s.sizeBytes = pos
	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	return nil
}
func (s *Segment) append(payload []byte) (relativeIndex int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame := EncodeFrame(payload)
	pos := s.writePos
	if _, err := s.file.Write(frame); err != nil {
		return 0, fmt.Errorf("segment append : %w", err)
	}
	s.filePos = append(s.filePos, pos)
	s.writePos += int64(len(frame))
	s.sizeBytes += int64(len(frame))
	return len(s.filePos) - 1, err
}

func (s *Segment) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Sync()
}

func (s *Segment) readAt(relativeIndex int) ([]byte, error) {
	s.mu.RLock()
	if relativeIndex < 0 || relativeIndex >= len(s.filePos) {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w:relative index %d out of range [0,%d)", ErrOffsetOutOfRange, relativeIndex, len(s.filePos))
	}
	pos := s.filePos[relativeIndex]
	s.mu.RUnlock()
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

func (s *Segment) recordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.filePos)
}

func (s *Segment) shouldRoll() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes >= s.maxBytes
}

func (s *Segment) close() error {
	return s.file.Close()
}
