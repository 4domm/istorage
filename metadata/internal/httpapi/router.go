package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"sstorage/metadata/internal/chunkers"
	"sstorage/metadata/internal/model"
	"sstorage/metadata/internal/service"
)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

func NewRouter(logger *slog.Logger, svc *service.Service, registry *chunkers.Registry) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(logger))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Route("/objects/{bucket}", func(r chi.Router) {
		r.Put("/*", putObject(svc))
		r.Get("/*", getObject(svc))
		r.Delete("/*", deleteObject(svc))
	})
	r.Get("/buckets/{bucket}/objects", listBucket(svc))
	r.Get("/chunks/{node_id}/{chunk_id}/in-use", chunkInUse(svc))
	r.Get("/chunkers", listChunkers(registry))
	r.Post("/placement", placement(registry))
	return r
}

func putObject(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, key, ok := objectPathParams(w, r)
		if !ok {
			return
		}

		var body model.PutObjectRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}

		if err := svc.PutObject(r.Context(), bucket, key, body); err != nil {
			writeInternalError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func getObject(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, key, ok := objectPathParams(w, r)
		if !ok {
			return
		}
		meta, err := svc.GetObject(r.Context(), bucket, key)
		if err != nil {
			if service.IsNotFound(err) {
				writeError(w, http.StatusNotFound, "object not found")
				return
			}
			writeInternalError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, meta)
	}
}

func deleteObject(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, key, ok := objectPathParams(w, r)
		if !ok {
			return
		}
		deleted, err := svc.DeleteObject(r.Context(), bucket, key)
		if err != nil {
			writeInternalError(w, err)
			return
		}
		if !deleted {
			writeError(w, http.StatusNotFound, "object not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func listBucket(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")
		if bucket == "" {
			writeError(w, http.StatusBadRequest, "bucket required")
			return
		}
		limit, err := normalizedLimit(r.URL.Query().Get("limit"), defaultListLimit, maxListLimit)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cursor := r.URL.Query().Get("cursor")
		keys, nextCursor, err := svc.ListBucketKeys(r.Context(), bucket, limit, cursor)
		if err != nil {
			writeInternalError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, model.ListBucketResponse{Keys: keys, NextCursor: nextCursor})
	}
}

func chunkInUse(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		inUse, err := svc.ChunkInUse(r.Context(), chi.URLParam(r, "node_id"), chi.URLParam(r, "chunk_id"))
		if err != nil {
			writeInternalError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, model.ChunkInUseResult{InUse: inUse})
	}
}

func listChunkers(registry *chunkers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		healthyOnly := r.URL.Query().Get("healthy_only") == "1" || strings.EqualFold(r.URL.Query().Get("healthy_only"), "true")
		nodes := registry.List(healthyOnly)
		out := make([]model.ChunkerNode, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, model.ChunkerNode{
				NodeID:  node.NodeID,
				BaseURL: node.BaseURL,
				Healthy: node.Healthy,
			})
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func placement(registry *chunkers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req model.PlacementRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		if len(req.ChunkIDs) == 0 {
			writeError(w, http.StatusBadRequest, "chunk_ids must be non-empty")
			return
		}
		placements, err := registry.PlaceChunks(req.ChunkIDs, true)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		out := make([]model.Placement, 0, len(req.ChunkIDs))
		for _, chunkID := range req.ChunkIDs {
			node, ok := placements[chunkID]
			if !ok {
				writeError(w, http.StatusInternalServerError, "placement is incomplete")
				return
			}
			out = append(out, model.Placement{
				ChunkID: chunkID,
				NodeID:  node.NodeID,
				BaseURL: node.BaseURL,
			})
		}
		writeJSON(w, http.StatusOK, model.PlacementResponse{Placements: out})
	}
}

func objectPathParams(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	bucket := chi.URLParam(r, "bucket")
	key := strings.TrimPrefix(chi.URLParam(r, "*"), "/")
	if bucket == "" || key == "" {
		writeError(w, http.StatusBadRequest, "bucket and key required")
		return "", "", false
	}
	return bucket, key, true
}

func normalizedLimit(raw string, defaultLimit, maxLimit int) (int, error) {
	if raw == "" {
		return defaultLimit, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.New("limit must be > 0")
	}
	if value > maxLimit {
		return maxLimit, nil
	}
	return value, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

func writeInternalError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, err.Error())
}

func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			logger.InfoContext(context.Background(), "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}
