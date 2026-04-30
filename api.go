package images

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func NewCoordinatorHandler(cfg CoordinatorConfig, registry *Registry) http.Handler {
	storage := NewStorageClient(cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/internal/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req HeartbeatRequest
		if err := DecodeJSON(r, &req); err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.ServerID == "" || req.URL == "" {
			WriteError(w, http.StatusBadRequest, "server_id and url are required")
			return
		}
		if err := registry.Heartbeat(req); err != nil {
			WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/internal/allocate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req AllocateRequest
		if err := DecodeJSON(r, &req); err != nil {
			WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := registry.Allocate(r.Context(), req.Size)
		if err != nil {
			WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/internal/lookup", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		packID, err := strconv.ParseUint(r.URL.Query().Get("pack_id"), 10, 32)
		if err != nil {
			WriteError(w, http.StatusBadRequest, "invalid pack_id")
			return
		}
		resp, err := registry.Lookup(uint32(packID))
		if err != nil {
			WriteError(w, http.StatusNotFound, err.Error())
			return
		}
		WriteJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/internal/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		WriteJSON(w, http.StatusOK, registry.Status())
	})
	mux.HandleFunc("/b/", func(w http.ResponseWriter, r *http.Request) {
		bucket, key, hasKey := parseBucketPath(strings.TrimPrefix(r.URL.Path, "/b/"))
		if bucket == "" {
			WriteError(w, http.StatusBadRequest, "bucket required")
			return
		}
		if redirectURL, ok := registry.RedirectURL(bucket, r.URL.RequestURI()); ok {
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return
		}
		if !registry.OwnsBucket(bucket) {
			WriteError(w, http.StatusServiceUnavailable, "bucket shard is not configured")
			return
		}

		switch {
		case !hasKey && r.Method == http.MethodGet:
			limit := 100
			if raw := r.URL.Query().Get("limit"); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
					limit = parsed
				}
			}
			records, err := registry.ListObjects(bucket, r.URL.Query().Get("prefix"), limit)
			if err != nil {
				WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			out := make([]map[string]any, 0, len(records))
			for _, record := range records {
				out = append(out, map[string]any{
					"bucket":       record.Bucket,
					"key":          record.Key,
					"blob_id":      record.BlobID,
					"url":          objectURL(record.Bucket, record.Key),
					"content_type": record.Metadata.ContentType,
					"size":         record.Metadata.Size,
					"updated_at":   record.UpdatedAt.UTC(),
				})
			}
			WriteJSON(w, http.StatusOK, map[string]any{"objects": out})
			return
		case !hasKey:
			WriteError(w, http.StatusBadRequest, "object key required")
			return
		}

		switch r.Method {
		case http.MethodPut:
			reader := http.MaxBytesReader(w, r.Body, cfg.MaxUploadBytes)
			defer reader.Close()
			body, err := io.ReadAll(reader)
			if err != nil {
				WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			if len(body) == 0 {
				WriteError(w, http.StatusBadRequest, "empty body")
				return
			}
			meta := DetectImageMetadata(body, r.Header.Get("Content-Type"))
			alloc, err := registry.Allocate(r.Context(), uint64(len(body)))
			if err != nil {
				WriteError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
			writeReq := EntryWriteRequest{
				EntryID:  alloc.EntryID,
				Guard:    alloc.Guard,
				Metadata: meta,
				Replicas: alloc.Replicas,
			}
			if err := storage.Write(alloc.Primary, alloc.PackID, writeReq, body); err != nil {
				WriteError(w, http.StatusBadGateway, err.Error())
				return
			}
			record := objectRecord{
				Bucket:    bucket,
				Key:       key,
				BlobID:    alloc.BlobID,
				Metadata:  meta,
				UpdatedAt: time.Now().UTC(),
			}
			previous, err := registry.PutObject(bucket, key, record)
			if err != nil {
				WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if previous != nil && previous.BlobID != "" && previous.BlobID != alloc.BlobID {
				_ = deleteBlobByID(storage, registry, previous.BlobID)
			}
			WriteJSON(w, http.StatusCreated, map[string]any{
				"bucket":       bucket,
				"key":          key,
				"blob_id":      alloc.BlobID,
				"url":          objectURL(bucket, key),
				"content_type": meta.ContentType,
				"size":         meta.Size,
				"checksum":     meta.Checksum,
			})
		case http.MethodGet, http.MethodHead:
			record, err := registry.GetObject(bucket, key)
			if err != nil {
				WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if record == nil {
				WriteError(w, http.StatusNotFound, "object not found")
				return
			}
			if err := serveBlobByID(storage, registry, record.BlobID, w, r); err != nil {
				WriteError(w, http.StatusNotFound, err.Error())
				return
			}
		case http.MethodDelete:
			record, err := registry.DeleteObject(bucket, key)
			if err != nil {
				WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if record == nil {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_ = deleteBlobByID(storage, registry, record.BlobID)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

func parseBucketPath(raw string) (bucket string, key string, hasKey bool) {
	trimmed := strings.TrimPrefix(raw, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return parts[0], "", false
	}
	return parts[0], normalizeObjectPath(parts[1]), true
}

func serveBlobByID(storage *StorageClient, registry *Registry, rawBlobID string, w http.ResponseWriter, r *http.Request) error {
	blobID, err := ParseBlobID(rawBlobID)
	if err != nil {
		return err
	}
	lookup, err := registry.Lookup(blobID.PackID)
	if err != nil {
		return err
	}
	replicas := append([]Replica{lookup.Primary}, lookup.Replicas...)
	for _, replica := range replicas {
		resp, readErr := storage.Read(replica, blobID.PackID, blobID.EntryID, blobID.Guard, r.Header)
		if readErr != nil {
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			continue
		}
		defer resp.Body.Close()
		CopyHeaders(w.Header(), resp.Header, "Content-Type", "Content-Length", "Content-Range", "ETag", "Cache-Control", "Content-Disposition")
		if w.Header().Get("Cache-Control") == "" {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		w.WriteHeader(resp.StatusCode)
		if r.Method == http.MethodGet {
			_, _ = io.Copy(w, resp.Body)
		}
		return nil
	}
	return errBlobNotFound
}

func deleteBlobByID(storage *StorageClient, registry *Registry, rawBlobID string) error {
	blobID, err := ParseBlobID(rawBlobID)
	if err != nil {
		return err
	}
	lookup, err := registry.Lookup(blobID.PackID)
	if err != nil {
		return err
	}
	req := EntryDeleteRequest{
		EntryID:  blobID.EntryID,
		Guard:    blobID.Guard,
		Replicas: lookup.Replicas,
	}
	return storage.Delete(lookup.Primary, blobID.PackID, req)
}

var errBlobNotFound = http.ErrMissingFile
