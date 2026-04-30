package images

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAllocateCreatesPack(t *testing.T) {
	cfg := CoordinatorConfig{
		DBPath:             filepath.Join(t.TempDir(), "badger"),
		PackSizeBytes:      1024,
		ReplicaCount:       1,
		ObjectCacheEntries: 8,
		ShardID:            0,
		ShardCount:         1,
		ShardURLs:          []string{"http://127.0.0.1:9000"},
	}
	registry, err := LoadRegistry(cfg)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	defer func() {
		if err := registry.Close(); err != nil {
			t.Fatalf("close registry: %v", err)
		}
	}()
	registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := registry.Heartbeat(HeartbeatRequest{
		ServerID:     "s1",
		URL:          "http://storage-a",
		FreeBytes:    1 << 20,
		MaxPackBytes: 1024,
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	resp, err := registry.Allocate(t.Context(), 10)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if resp.PackID == 0 || resp.EntryID == 0 || resp.BlobID == "" {
		t.Fatalf("unexpected allocation response: %+v", resp)
	}
}

func TestObjectCatalogRoundTripAndList(t *testing.T) {
	cfg := CoordinatorConfig{
		DBPath:             filepath.Join(t.TempDir(), "badger"),
		ObjectCacheEntries: 4,
		ShardID:            0,
		ShardCount:         2,
		ShardURLs:          []string{"http://coord-a:9000", "http://coord-b:9000"},
	}
	registry, err := LoadRegistry(cfg)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	defer func() {
		if err := registry.Close(); err != nil {
			t.Fatalf("close registry: %v", err)
		}
	}()

	ownerA, ok := registry.ShardOwner("images-demo")
	if !ok {
		t.Fatal("expected shard owner")
	}
	redirectURL, shouldRedirect := registry.RedirectURL("images-demo", "/b/images-demo/cats/one.jpg")
	if ownerA == cfg.ShardID {
		if !registry.OwnsBucket("images-demo") || shouldRedirect {
			t.Fatal("expected local bucket ownership without redirect")
		}
	} else {
		if registry.OwnsBucket("images-demo") || !shouldRedirect || redirectURL == "" {
			t.Fatal("expected redirect for foreign bucket shard")
		}
	}

	now := time.Now().UTC().Round(time.Second)
	record := objectRecord{
		Bucket: "images-demo",
		Key:    "cats/one.jpg",
		BlobID: "00000001,0000000000000001,00000001",
		Metadata: ImageMetadata{
			ContentType: "image/jpeg",
			Size:        123,
		},
		UpdatedAt: now,
	}
	prev, err := registry.PutObject(record.Bucket, record.Key, record)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	if prev != nil {
		t.Fatalf("expected no previous record, got %+v", prev)
	}

	got, err := registry.GetObject(record.Bucket, record.Key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if got == nil || got.BlobID != record.BlobID || got.Key != record.Key {
		t.Fatalf("unexpected object: %+v", got)
	}

	second := objectRecord{
		Bucket: "images-demo",
		Key:    "cats/two.jpg",
		BlobID: "00000001,0000000000000002,00000002",
		Metadata: ImageMetadata{
			ContentType: "image/jpeg",
			Size:        456,
		},
		UpdatedAt: now,
	}
	if _, err := registry.PutObject(second.Bucket, second.Key, second); err != nil {
		t.Fatalf("put second object: %v", err)
	}

	list, err := registry.ListObjects("images-demo", "cats/", 10)
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(list))
	}

	deleted, err := registry.DeleteObject(record.Bucket, record.Key)
	if err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if deleted == nil || deleted.BlobID != record.BlobID {
		t.Fatalf("unexpected deleted record: %+v", deleted)
	}
	missing, err := registry.GetObject(record.Bucket, record.Key)
	if err != nil {
		t.Fatalf("get deleted object: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected object to be deleted, got %+v", missing)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
