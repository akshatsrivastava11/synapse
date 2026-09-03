package objectstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func fakeS3Server(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	objects := make(map[string][]byte)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing Authorization header", http.StatusForbidden)
			return
		}
		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			objects[r.URL.Path] = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, ok := objects[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(data)
		case http.MethodDelete:
			delete(objects, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		}
	}))
}

func TestS3ClientPutGetDelete(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()

	endpoint := strings.TrimPrefix(srv.URL, "http://")
	client := NewS3Client(endpoint, "test-access-key", "test-secret-key", "streamdb", false)
	ctx := context.Background()
	key := "partition-0/00000000000000000000.log"
	payload := []byte("hello from a segment file")

	if err := client.Put(ctx, key, payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Get: got %q, want %q", got, payload)
	}

	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := client.Get(ctx, key); err != ErrNotFound {
		t.Fatalf("Get after Delete: got err %v, want ErrNotFound", err)
	}
}
