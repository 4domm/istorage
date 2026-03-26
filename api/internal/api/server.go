package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"sstorage/api/internal/chunk"
	"sstorage/api/internal/config"
	apierrors "sstorage/api/internal/errors"
	"sstorage/api/internal/metadata"
	"sstorage/api/internal/model"
)

const putUploadConcurrency = 16
const placementBatchSize = 64

type Server struct {
	cfg      config.Config
	logger   *slog.Logger
	metadata *metadata.Client
	chunk    *chunk.Client
}

type uploadResult struct {
	nodeID  string
	chunkID string
	created bool
	err     error
}

type pendingChunk struct {
	chunkID string
	data    []byte
	offset  uint64
}

func New(cfg config.Config, logger *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		logger:   logger,
		metadata: metadata.New(cfg.MetadataServiceURL),
		chunk:    chunk.New(),
	}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			s.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
		}()

		if r.URL.Path == "/" {
			http.NotFound(w, r)
			return
		}

		trimmed := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(trimmed, "/", 2)
		bucket := parts[0]
		key := ""
		if len(parts) == 2 {
			key = parts[1]
		}

		switch {
		case r.Method == http.MethodGet && key == "":
			s.handleListBucket(w, r, bucket)
		case key != "":
			switch r.Method {
			case http.MethodPut:
				s.handlePutObject(w, r, bucket, key)
			case http.MethodGet:
				s.handleGetObject(w, r, bucket, key)
			case http.MethodDelete:
				s.handleDeleteObject(w, r, bucket, key)
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	})
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := validateBucket(bucket); err != nil {
		apierrors.Write(w, err)
		return
	}
	if key == "" {
		apierrors.Write(w, apierrors.Validation("key must be non-empty"))
		return
	}
	if s.cfg.ChunkSize <= 0 {
		apierrors.Write(w, apierrors.Validation("chunk_size must be > 0"))
		return
	}

	ctx := r.Context()

	reader := r.Body
	defer reader.Close()

	manifest := make([]model.ChunkRef, 0)
	uploadedChunks := make([][2]string, 0)
	results := make(chan uploadResult, putUploadConcurrency)
	sem := make(chan struct{}, putUploadConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex
	placementCache := make(map[string]model.ChunkerNode)
	pending := make([]pendingChunk, 0, placementBatchSize)

	recordErr := func(err error) {
		firstErrMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		firstErrMu.Unlock()
	}

	hasher := sha256.New()
	offset := uint64(0)
	buf := make([]byte, s.cfg.ChunkSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if readErr == io.EOF {
			break
		}
		if readErr == io.ErrUnexpectedEOF && n > 0 {
			hasher.Write(buf[:n])
			chunkData := append([]byte(nil), buf[:n]...)
			chunkHashBytes := sha256.Sum256(chunkData)
			pending = append(pending, pendingChunk{
				chunkID: hex.EncodeToString(chunkHashBytes[:]),
				data:    chunkData,
				offset:  offset,
			})
			offset += uint64(n)
			break
		}
		if readErr != nil {
			apierrors.Write(w, apierrors.Internal(readErr.Error()))
			return
		}
		hasher.Write(buf[:n])
		chunkData := append([]byte(nil), buf[:n]...)
		chunkHashBytes := sha256.Sum256(chunkData)
		pending = append(pending, pendingChunk{
			chunkID: hex.EncodeToString(chunkHashBytes[:]),
			data:    chunkData,
			offset:  offset,
		})
		offset += uint64(n)
		if len(pending) >= placementBatchSize {
			if err := s.dispatchUploadBatch(ctx, pending, placementCache, &manifest, &wg, sem, results); err != nil {
				recordErr(err)
				break
			}
			pending = pending[:0]
		}
	}

	if firstErr == nil && len(pending) > 0 {
		if err := s.dispatchUploadBatch(ctx, pending, placementCache, &manifest, &wg, sem, results); err != nil {
			recordErr(err)
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.err != nil {
			recordErr(result.err)
			continue
		}
		if result.created {
			uploadedChunks = append(uploadedChunks, [2]string{result.nodeID, result.chunkID})
		}
	}

	if firstErr != nil {
		s.rollbackNewChunks(ctx, uploadedChunks)
		apierrors.Write(w, apierrors.Internal(firstErr.Error()))
		return
	}

	etag := "\"" + hex.EncodeToString(hasher.Sum(nil)) + "\""
	req := model.PutObjectRequest{
		Size:     offset,
		ETag:     etag,
		Manifest: manifest,
	}
	if err := s.metadata.PutObject(ctx, bucket, key, req); err != nil {
		s.rollbackNewChunks(ctx, uploadedChunks)
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) dispatchUploadBatch(
	ctx context.Context,
	pending []pendingChunk,
	placementCache map[string]model.ChunkerNode,
	manifest *[]model.ChunkRef,
	wg *sync.WaitGroup,
	sem chan struct{},
	results chan uploadResult,
) error {
	missing := make([]string, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, item := range pending {
		if _, ok := placementCache[item.chunkID]; ok {
			continue
		}
		if _, ok := seen[item.chunkID]; ok {
			continue
		}
		seen[item.chunkID] = struct{}{}
		missing = append(missing, item.chunkID)
	}

	if len(missing) > 0 {
		placements, err := s.metadata.PlaceChunks(ctx, missing)
		if err != nil {
			return err
		}
		for chunkID, node := range placements {
			placementCache[chunkID] = node
		}
	}

	for _, item := range pending {
		node, ok := placementCache[item.chunkID]
		if !ok {
			return fmt.Errorf("metadata placement missing chunk %s", item.chunkID)
		}
		*manifest = append(*manifest, model.ChunkRef{
			ChunkID: item.chunkID,
			NodeID:  node.NodeID,
			Offset:  item.offset,
			Size:    uint64(len(item.data)),
		})
		sem <- struct{}{}
		wg.Add(1)
		go func(baseURL, nodeID, chunkID string, payload []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			created, err := s.chunk.PutChunk(ctx, baseURL, chunkID, payload)
			results <- uploadResult{nodeID: nodeID, chunkID: chunkID, created: created, err: err}
		}(node.BaseURL, node.NodeID, item.chunkID, item.data)
	}
	return nil
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := validateBucket(bucket); err != nil {
		apierrors.Write(w, err)
		return
	}
	ctx := r.Context()
	meta, err := s.metadata.GetObject(ctx, bucket, key)
	if err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}
	if meta == nil {
		apierrors.Write(w, apierrors.NotFound(fmt.Sprintf("object not found: %s/%s", bucket, key)))
		return
	}
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" && etagMatches(ifNoneMatch, meta.ETag) {
		w.Header().Set("ETag", meta.ETag)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	nodes, err := s.metadata.ListChunkerNodes(ctx, false)
	if err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}
	nodeMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		nodeMap[node.NodeID] = node.BaseURL
	}

	w.Header().Set("Content-Length", strconv.FormatUint(meta.Size, 10))
	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)

	if len(meta.Manifest) == 1 {
		chunkRef := meta.Manifest[0]
		baseURL, ok := nodeMap[chunkRef.NodeID]
		if !ok {
			return
		}
		payload, err := s.chunk.GetChunk(ctx, baseURL, chunkRef.ChunkID)
		if err != nil {
			return
		}
		_, _ = w.Write(payload)
		return
	}

	windowSize := s.cfg.GetBatchWindow
	if windowSize < 1 {
		windowSize = 1
	}
	for start := 0; start < len(meta.Manifest); start += windowSize {
		end := start + windowSize
		if end > len(meta.Manifest) {
			end = len(meta.Manifest)
		}
		batches, err := s.fetchWindowBatches(ctx, nodeMap, meta.Manifest[start:end])
		if err != nil {
			return
		}
		for _, chunkRef := range meta.Manifest[start:end] {
			chunks := batches[chunkRef.NodeID]
			payload, ok := chunks[chunkRef.ChunkID]
			if !ok {
				return
			}
			if _, err := w.Write(payload); err != nil {
				return
			}
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

func (s *Server) fetchWindowBatches(ctx context.Context, nodeMap map[string]string, window []model.ChunkRef) (map[string]map[string][]byte, error) {
	idsByNode := make(map[string]map[string]struct{})
	for _, chunkRef := range window {
		if idsByNode[chunkRef.NodeID] == nil {
			idsByNode[chunkRef.NodeID] = map[string]struct{}{}
		}
		idsByNode[chunkRef.NodeID][chunkRef.ChunkID] = struct{}{}
	}

	type nodeResult struct {
		nodeID string
		chunks map[string][]byte
		err    error
	}
	results := make(chan nodeResult, len(idsByNode))
	var wg sync.WaitGroup

	for nodeID, chunkSet := range idsByNode {
		baseURL, ok := nodeMap[nodeID]
		if !ok {
			return nil, fmt.Errorf("missing chunker node %s for batch get", nodeID)
		}
		chunkIDs := make([]string, 0, len(chunkSet))
		for chunkID := range chunkSet {
			chunkIDs = append(chunkIDs, chunkID)
		}
		wg.Add(1)
		go func(nodeID, baseURL string, ids []string) {
			defer wg.Done()
			chunks, err := s.chunk.BatchGetChunks(ctx, baseURL, ids)
			results <- nodeResult{nodeID: nodeID, chunks: chunks, err: err}
		}(nodeID, baseURL, chunkIDs)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	batches := make(map[string]map[string][]byte, len(idsByNode))
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		batches[result.nodeID] = result.chunks
	}
	return batches, nil
}

func (s *Server) handleListBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := validateBucket(bucket); err != nil {
		apierrors.Write(w, err)
		return
	}

	var limit *int
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			apierrors.Write(w, apierrors.Validation("limit must be a positive integer"))
			return
		}
		limit = &value
	}
	var cursor *string
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cursor = &raw
	}
	response, err := s.metadata.ListBucketKeys(r.Context(), bucket, limit, cursor)
	if err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := validateBucket(bucket); err != nil {
		apierrors.Write(w, err)
		return
	}
	deleted, err := s.metadata.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}
	if !deleted {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) rollbackNewChunks(ctx context.Context, uploadedChunks [][2]string) {
	nodes, err := s.metadata.ListChunkerNodes(ctx, false)
	if err != nil {
		s.logger.Warn("failed to list chunker nodes for rollback", "error", err)
		return
	}
	nodeMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		nodeMap[node.NodeID] = node.BaseURL
	}
	for _, entry := range uploadedChunks {
		nodeID, chunkID := entry[0], entry[1]
		inUse, err := s.metadata.ChunkInUse(ctx, nodeID, chunkID)
		if err != nil || inUse {
			continue
		}
		baseURL, ok := nodeMap[nodeID]
		if !ok {
			continue
		}
		_ = s.chunk.DeleteChunk(ctx, baseURL, chunkID)
	}
}

func validateBucket(bucket string) error {
	if bucket == "" || strings.Contains(bucket, "/") || strings.HasPrefix(bucket, ".") {
		return apierrors.Validation("invalid bucket name")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func etagMatches(headerValue, objectETag string) bool {
	for _, raw := range strings.Split(headerValue, ",") {
		candidate := strings.TrimSpace(raw)
		if candidate == "*" || candidate == objectETag {
			return true
		}
	}
	return false
}
