package objectstore

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu          sync.Mutex
	data        map[string][]byte
	PutCount    int
	GetCount    int
	DeleteCount int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (m *MemoryStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PutCount++

	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}
func (m *MemoryStore) Get(_ context.Context, key string) (data []byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.GetCount++
	v, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteCount++
	delete(m.data, key)
	return nil
}

func (m *MemoryStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}
