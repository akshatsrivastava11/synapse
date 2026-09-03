package broker

import (
	"akshat/synapse/internal/objectstore"
	"akshat/synapse/internal/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Server struct {
	dataDir         string
	maxSegmentBytes int64
	store           objectstore.ObjectStore
	flushInterval   time.Duration
	mu              sync.RWMutex
	partitions      map[string]*storage.Partition
}

func NewServer(dataDir string, maxSegmentBytes int64, store objectstore.ObjectStore, flushIntervals time.Duration) *Server {
	return &Server{
		dataDir:         dataDir,
		maxSegmentBytes: maxSegmentBytes,
		store:           store,
		flushInterval:   flushIntervals,
		partitions:      make(map[string]*storage.Partition),
	}
}

func partitionKey(topic string, partitionID int) string {
	return fmt.Sprintf("%s/%d", topic, partitionID)
}

func (s *Server) getOrOpenPartition(topic string, partitionId int) (*storage.Partition, error) {
	key := partitionKey(topic, partitionId)
	s.mu.RLock()
	p, ok := s.partitions[key]
	s.mu.RUnlock()
	if ok {
		return p, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-checked locking: another goroutine may have created it while we waited.
	if p, ok := s.partitions[key]; ok {
		return p, nil
	}

	topicDir := filepath.Join(s.dataDir, topic)
	p, err := storage.OpenPartition(topicDir, partitionId, storage.PartitionObject{
		MaxSegmentBytes: s.maxSegmentBytes,
		Store:           s.store,
	})
	if err != nil {
		return nil, fmt.Errorf("open partition %s: %w", key, err)
	}
	s.partitions[key] = p
	log.Printf("opened partition %s", key)
	return p, nil

}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", s.handleProduce)
	mux.HandleFunc("GET /fetch", s.handleFetch)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

type produceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing required query param: topic", http.StatusBadRequest)
		return
	}
	partitionID, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil {
		http.Error(w, "missing or invalid required query param: partition", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024*1024)) // 16MB cap per record - sane MVP limit
	if err != nil {
		http.Error(w, fmt.Sprintf("read request body: %v", err), http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty record body", http.StatusBadRequest)
		return
	}

	p, err := s.getOrOpenPartition(topic, partitionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	offset, err := p.Append(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("append: %v", err), http.StatusInternalServerError)
		return
	}
	if err := p.Sync(); err != nil {
		http.Error(w, fmt.Sprintf("sync: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(produceResponse{Topic: topic, Partition: partitionID, Offset: offset})
	log.Printf("[PRODUCE] topic=%s partition=%d offset=%d size=%d bytes", topic, partitionID, offset, len(body))
}

func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing required query param: topic", http.StatusBadRequest)
		return
	}
	partitionID, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil {
		http.Error(w, "missing or invalid required query param: partition", http.StatusBadRequest)
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		http.Error(w, "missing or invalid required query param: offset", http.StatusBadRequest)
		return
	}

	p, err := s.getOrOpenPartition(topic, partitionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := p.ReadFrom(r.Context(), offset)
	if err != nil {
		if errors.Is(err, storage.ErrOffsetOutOfRange) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("fetch: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(payload)
	log.Printf("[FETCH] topic=%s partition=%d offset=%d size=%d bytes", topic, partitionID, offset, len(payload))
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) StartFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.flushAllPartitions(ctx)
		}
	}
}

func (s *Server) flushAllPartitions(ctx context.Context) {
	s.mu.RLock()
	parts := make(map[string]*storage.Partition, len(s.partitions))
	for k, p := range s.partitions {
		parts[k] = p
	}
	s.mu.RUnlock()

	for key, p := range parts {
		n, err := p.FlushSealedSegments(ctx)
		if err != nil {
			log.Printf("flush error for partition %s: %v", key, err)
			continue
		}
		if n > 0 {
			log.Printf("partition %s: flushed %d segment(s) to object storage", key, n)
		}
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for key, p := range s.partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close partition %s: %w", key, err)
		}
	}
	return firstErr
}
