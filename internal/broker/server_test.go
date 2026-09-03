package broker

import (
	"akshat/synapse/internal/objectstore"
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBrokerServerEndpoints(t *testing.T) {
	dir := t.TempDir()
	store := objectstore.NewMemoryStore()
	srv := NewServer(dir, 1024, store, 100*time.Millisecond)
	defer srv.Close()

	handler := srv.Routes()

	// 1. Test GET /healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status: got %d, want %d", rec.Code, http.StatusOK)
	}

	// 2. Test POST /produce
	body := []byte("hello streamdb")
	req = httptest.NewRequest(http.MethodPost, "/produce?topic=test-topic&partition=0", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("produce status: got %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// 3. Test GET /fetch
	req = httptest.NewRequest(http.MethodGet, "/fetch?topic=test-topic&partition=0&offset=0", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch status: got %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "hello streamdb" {
		t.Fatalf("fetch content: got %q, want %q", rec.Body.String(), "hello streamdb")
	}

	// 4. Test GET /fetch out of range
	req = httptest.NewRequest(http.MethodGet, "/fetch?topic=test-topic&partition=0&offset=999", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("fetch out of range status: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}
