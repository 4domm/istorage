package api

import (
	"bytes"
	"context"
	"crypto/rand"
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

	"github.com/klauspost/reedsolomon"

	"istorage/api/internal/chunk"
	"istorage/api/internal/config"
	apierrors "istorage/api/internal/errors"
	"istorage/api/internal/metadata"
	"istorage/api/internal/model"
)

const (
	putUploadConcurrency = 16
)

type Server struct {
	cfg          config.Config
	logger       *slog.Logger
	metadata     *metadata.Client
	chunk        *chunk.Client
	chunkBufPool sync.Pool
}

type uploadResult struct {
	nodeID  string
	chunkID string
	err     error
}

type pendingShardUpload struct {
	chunkID    string
	shardIndex int
	data       []byte
}

type pendingChunkUpload struct {
	offset       uint64
	size         uint64
	dataShards   int
	parityShards int
	shards       []pendingShardUpload
	pooledData   []byte
}

type chunkIDGenerator struct {
	prefix string
	index  uint64
}

func newChunkIDGenerator() (*chunkIDGenerator, error) {
	seed := make([]byte, 16)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	return &chunkIDGenerator{prefix: hex.EncodeToString(seed)}, nil
}

func (g *chunkIDGenerator) Next() string {
	id := fmt.Sprintf("%s-%016x", g.prefix, g.index)
	g.index++
	return id
}

func New(cfg config.Config, logger *slog.Logger) *Server {
	pool := sync.Pool{
		New: func() any {
			return make([]byte, cfg.ChunkSize)
		},
	}
	return &Server{
		cfg:          cfg,
		logger:       logger,
		metadata:     metadata.New(cfg.MetadataServiceURL),
		chunk:        chunk.New(),
		chunkBufPool: pool,
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

	placementBatchSize := s.cfg.PutPlacementBatchSize
	if placementBatchSize < 1 {
		placementBatchSize = 32
	}

	manifest := make([]model.ChunkRef, 0)
	pending := make([]pendingChunkUpload, 0, placementBatchSize)
	uploadedChunks := make([][2]string, 0)
	results := make(chan uploadResult, putUploadConcurrency)
	sem := make(chan struct{}, putUploadConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex
	idGen, err := newChunkIDGenerator()
	if err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}

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
			chunkData := s.acquireChunkBuffer(n)
			copy(chunkData, buf[:n])
			pendingUpload, prepareErr := s.prepareChunkUpload(chunkData, offset, idGen)
			if prepareErr != nil {
				s.releaseChunkBuffer(chunkData)
				recordErr(prepareErr)
			} else {
				pending = append(pending, pendingUpload)
			}
			offset += uint64(n)
			break
		}
		if readErr != nil {
			apierrors.Write(w, apierrors.Internal(readErr.Error()))
			return
		}
		hasher.Write(buf[:n])
		chunkData := s.acquireChunkBuffer(n)
		copy(chunkData, buf[:n])
		pendingUpload, prepareErr := s.prepareChunkUpload(chunkData, offset, idGen)
		if prepareErr != nil {
			s.releaseChunkBuffer(chunkData)
			recordErr(prepareErr)
			break
		}
		pending = append(pending, pendingUpload)
		offset += uint64(n)
		if len(pending) >= placementBatchSize {
			refs, dispatchErr := s.dispatchUploadBatch(ctx, pending, &wg, sem, results)
			if dispatchErr != nil {
				recordErr(dispatchErr)
				break
			}
			manifest = append(manifest, refs...)
			pending = pending[:0]
		}
	}

	if firstErr == nil && len(pending) > 0 {
		refs, dispatchErr := s.dispatchUploadBatch(ctx, pending, &wg, sem, results)
		if dispatchErr != nil {
			recordErr(dispatchErr)
		} else {
			manifest = append(manifest, refs...)
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
		uploadedChunks = append(uploadedChunks, [2]string{result.nodeID, result.chunkID})
	}

	if firstErr != nil {
		s.releasePendingPooledBuffers(pending)
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

func (s *Server) prepareChunkUpload(
	chunkData []byte,
	offset uint64,
	idGen *chunkIDGenerator,
) (pendingChunkUpload, error) {
	dataShards, parityShards := s.normalizedECParams()
	shards, err := s.encodeChunkShards(chunkData, dataShards, parityShards)
	if err != nil {
		return pendingChunkUpload{}, err
	}
	pooledData := []byte(nil)
	if dataShards == 1 && parityShards == 0 {
		pooledData = chunkData
	} else {
		s.releaseChunkBuffer(chunkData)
	}
	pendingShards := make([]pendingShardUpload, 0, len(shards))
	for _, shard := range shards {
		pendingShards = append(pendingShards, pendingShardUpload{
			chunkID:    idGen.Next(),
			shardIndex: shard.shardIndex,
			data:       shard.data,
		})
	}
	return pendingChunkUpload{
		offset:       offset,
		size:         uint64(len(chunkData)),
		dataShards:   dataShards,
		parityShards: parityShards,
		shards:       pendingShards,
		pooledData:   pooledData,
	}, nil
}

func (s *Server) dispatchUploadBatch(
	ctx context.Context,
	pending []pendingChunkUpload,
	wg *sync.WaitGroup,
	sem chan struct{},
	results chan uploadResult,
) ([]model.ChunkRef, error) {
	chunkIDs := make([]string, 0)
	for _, upload := range pending {
		for _, shard := range upload.shards {
			chunkIDs = append(chunkIDs, shard.chunkID)
		}
	}
	placements, err := s.metadata.PlaceChunks(ctx, chunkIDs)
	if err != nil {
		return nil, err
	}

	refs := make([]model.ChunkRef, 0, len(pending))
	type nodeBatch struct {
		baseURL string
		chunks  map[string][]byte
		pooled  [][]byte
	}
	byNode := make(map[string]*nodeBatch)
	for _, upload := range pending {
		ref := model.ChunkRef{
			Offset:       upload.offset,
			Size:         upload.size,
			DataShards:   upload.dataShards,
			ParityShards: upload.parityShards,
			Shards:       make([]model.ShardRef, 0, len(upload.shards)),
		}
		for _, shard := range upload.shards {
			node, ok := placements[shard.chunkID]
			if !ok {
				return nil, fmt.Errorf("metadata placement missing chunk %s", shard.chunkID)
			}
			ref.Shards = append(ref.Shards, model.ShardRef{
				ChunkID:    shard.chunkID,
				NodeID:     node.NodeID,
				ShardIndex: shard.shardIndex,
			})
			batch := byNode[node.NodeID]
			if batch == nil {
				batch = &nodeBatch{
					baseURL: node.BaseURL,
					chunks:  make(map[string][]byte),
					pooled:  make([][]byte, 0),
				}
				byNode[node.NodeID] = batch
			}
			batch.chunks[shard.chunkID] = shard.data
		}
		if upload.pooledData != nil && len(upload.shards) > 0 {
			firstNode, ok := placements[upload.shards[0].chunkID]
			if ok {
				batch := byNode[firstNode.NodeID]
				if batch != nil {
					batch.pooled = append(batch.pooled, upload.pooledData)
				}
			}
		}
		refs = append(refs, ref)
	}

	for nodeID, batch := range byNode {
		nodeID := nodeID
		batch := batch
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				for _, buf := range batch.pooled {
					s.releaseChunkBuffer(buf)
				}
			}()
			err := s.chunk.BatchPutChunks(ctx, batch.baseURL, batch.chunks)
			for chunkID := range batch.chunks {
				results <- uploadResult{nodeID: nodeID, chunkID: chunkID, err: err}
			}
		}()
	}
	return refs, nil
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
	byteRange, hasRange, err := parseSingleByteRange(r.Header.Get("Range"), meta.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
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

	startOffset := uint64(0)
	endOffset := uint64(0)
	status := http.StatusOK
	if meta.Size > 0 {
		endOffset = meta.Size - 1
	}
	if hasRange && meta.Size > 0 {
		startOffset = byteRange.start
		endOffset = byteRange.end
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startOffset, endOffset, meta.Size))
	}
	if err := s.preflightObjectRange(ctx, nodeMap, meta.Manifest, startOffset, endOffset); err != nil {
		apierrors.Write(w, apierrors.Internal(err.Error()))
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", meta.ETag)
	contentLen := uint64(0)
	if meta.Size > 0 {
		contentLen = endOffset - startOffset + 1
	}
	w.Header().Set("Content-Length", strconv.FormatUint(contentLen, 10))
	w.WriteHeader(status)

	if err := s.streamObjectRange(ctx, w, nodeMap, meta.Manifest, startOffset, endOffset); err != nil {
		return
	}
}

func (s *Server) preflightObjectRange(
	ctx context.Context,
	nodeMap map[string]string,
	manifest []model.ChunkRef,
	startOffset uint64,
	endOffset uint64,
) error {
	if len(manifest) == 0 {
		return nil
	}

	idsByNode := make(map[string]map[string]struct{})
	for _, chunkRef := range manifest {
		if chunkRef.Size == 0 {
			continue
		}
		chunkStart := chunkRef.Offset
		chunkEnd := chunkRef.Offset + chunkRef.Size - 1
		if chunkEnd < startOffset || chunkStart > endOffset {
			continue
		}
		for _, shard := range s.chunkShards(chunkRef) {
			if idsByNode[shard.NodeID] == nil {
				idsByNode[shard.NodeID] = map[string]struct{}{}
			}
			idsByNode[shard.NodeID][shard.ChunkID] = struct{}{}
		}
	}

	type nodeResult struct {
		nodeID string
		exists map[string]bool
		err    error
	}
	results := make(chan nodeResult, len(idsByNode))
	var wg sync.WaitGroup

	for nodeID, chunkSet := range idsByNode {
		baseURL, ok := nodeMap[nodeID]
		if !ok {
			return fmt.Errorf("missing chunker node %s for preflight", nodeID)
		}
		chunkIDs := make([]string, 0, len(chunkSet))
		for chunkID := range chunkSet {
			chunkIDs = append(chunkIDs, chunkID)
		}
		wg.Add(1)
		go func(nodeID, baseURL string, ids []string) {
			defer wg.Done()
			exists, err := s.chunk.BatchExistsChunks(ctx, baseURL, ids)
			if err != nil {
				results <- nodeResult{nodeID: nodeID, err: err}
				return
			}
			results <- nodeResult{nodeID: nodeID, exists: exists}
		}(nodeID, baseURL, chunkIDs)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	availability := make(map[string]map[string]bool, len(idsByNode))
	for result := range results {
		if result.err != nil {
			return result.err
		}
		availability[result.nodeID] = result.exists
	}

	for _, chunkRef := range manifest {
		if chunkRef.Size == 0 {
			continue
		}
		chunkStart := chunkRef.Offset
		chunkEnd := chunkRef.Offset + chunkRef.Size - 1
		if chunkEnd < startOffset || chunkStart > endOffset {
			continue
		}
		shards := s.chunkShards(chunkRef)
		dataShards := chunkRef.DataShards
		if dataShards < 1 {
			dataShards = 1
		}
		available := 0
		for _, shard := range shards {
			existsByNode := availability[shard.NodeID]
			if existsByNode != nil && existsByNode[shard.ChunkID] {
				available++
			}
		}
		if available < dataShards {
			return fmt.Errorf("insufficient shards for object chunk at offset %d", chunkRef.Offset)
		}
	}
	return nil
}

func (s *Server) streamObjectRange(
	ctx context.Context,
	w http.ResponseWriter,
	nodeMap map[string]string,
	manifest []model.ChunkRef,
	startOffset uint64,
	endOffset uint64,
) error {
	if len(manifest) == 0 {
		return nil
	}

	windowSize := s.cfg.GetBatchWindow
	if windowSize < 1 {
		windowSize = 1
	}
	for start := 0; start < len(manifest); start += windowSize {
		end := start + windowSize
		if end > len(manifest) {
			end = len(manifest)
		}
		window := manifest[start:end]
		type windowEntry struct {
			chunk model.ChunkRef
		}
		entries := make([]windowEntry, 0, len(window))
		for _, chunkRef := range window {
			if chunkRef.Size == 0 {
				continue
			}
			chunkStart := chunkRef.Offset
			chunkEnd := chunkRef.Offset + chunkRef.Size - 1
			if chunkEnd < startOffset || chunkStart > endOffset {
				continue
			}
			entries = append(entries, windowEntry{chunk: chunkRef})
		}

		type readResult struct {
			index   int
			payload []byte
			err     error
		}
		results := make(chan readResult, len(entries))
		var wg sync.WaitGroup
		for i, entry := range entries {
			wg.Add(1)
			go func(index int, ref model.ChunkRef) {
				defer wg.Done()
				payload, err := s.readChunkPayload(ctx, nodeMap, ref)
				results <- readResult{index: index, payload: payload, err: err}
			}(i, entry.chunk)
		}
		go func() {
			wg.Wait()
			close(results)
		}()

		payloadByIndex := make([][]byte, len(entries))
		for result := range results {
			if result.err != nil {
				return result.err
			}
			payloadByIndex[result.index] = result.payload
		}

		for i, entry := range entries {
			chunkRef := entry.chunk
			payload := payloadByIndex[i]
			if payload == nil {
				return fmt.Errorf("missing payload for chunk at offset %d", chunkRef.Offset)
			}

			chunkStart := chunkRef.Offset
			chunkEnd := chunkRef.Offset + chunkRef.Size - 1
			payloadStart := uint64(0)
			if startOffset > chunkStart {
				payloadStart = startOffset - chunkStart
			}
			payloadEnd := chunkRef.Size
			if endOffset < chunkEnd {
				payloadEnd = endOffset - chunkStart + 1
			}
			if payloadStart > payloadEnd || payloadEnd > uint64(len(payload)) {
				return fmt.Errorf("invalid range slice for chunk at offset %d", chunkRef.Offset)
			}
			if _, err := w.Write(payload[payloadStart:payloadEnd]); err != nil {
				return err
			}
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	return nil
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

func (s *Server) acquireChunkBuffer(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size > s.cfg.ChunkSize {
		return make([]byte, size)
	}
	buf := s.chunkBufPool.Get().([]byte)
	return buf[:size]
}

func (s *Server) releaseChunkBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) != s.cfg.ChunkSize {
		return
	}
	s.chunkBufPool.Put(buf[:s.cfg.ChunkSize])
}

func (s *Server) releasePendingPooledBuffers(pending []pendingChunkUpload) {
	for _, upload := range pending {
		if upload.pooledData != nil {
			s.releaseChunkBuffer(upload.pooledData)
		}
	}
}

type parsedByteRange struct {
	start uint64
	end   uint64
}

func parseSingleByteRange(header string, size uint64) (parsedByteRange, bool, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return parsedByteRange{}, false, nil
	}
	if size == 0 || !strings.HasPrefix(header, "bytes=") {
		return parsedByteRange{}, false, fmt.Errorf("invalid range")
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return parsedByteRange{}, false, fmt.Errorf("invalid range")
	}
	startRaw, endRaw, ok := strings.Cut(spec, "-")
	if !ok {
		return parsedByteRange{}, false, fmt.Errorf("invalid range")
	}
	startRaw = strings.TrimSpace(startRaw)
	endRaw = strings.TrimSpace(endRaw)

	if startRaw == "" {
		suffixLen, err := strconv.ParseUint(endRaw, 10, 64)
		if err != nil || suffixLen == 0 {
			return parsedByteRange{}, false, fmt.Errorf("invalid range")
		}
		if suffixLen >= size {
			return parsedByteRange{start: 0, end: size - 1}, true, nil
		}
		return parsedByteRange{start: size - suffixLen, end: size - 1}, true, nil
	}

	start, err := strconv.ParseUint(startRaw, 10, 64)
	if err != nil || start >= size {
		return parsedByteRange{}, false, fmt.Errorf("invalid range")
	}

	if endRaw == "" {
		return parsedByteRange{start: start, end: size - 1}, true, nil
	}

	end, err := strconv.ParseUint(endRaw, 10, 64)
	if err != nil || end < start {
		return parsedByteRange{}, false, fmt.Errorf("invalid range")
	}
	if end >= size {
		end = size - 1
	}
	return parsedByteRange{start: start, end: end}, true, nil
}

type shardPayload struct {
	chunkID    string
	shardIndex int
	data       []byte
}

func (s *Server) normalizedECParams() (int, int) {
	dataShards := s.cfg.ECDataShards
	if dataShards < 1 {
		dataShards = 1
	}
	parityShards := s.cfg.ECParityShards
	if parityShards < 0 {
		parityShards = 0
	}
	if dataShards == 1 && parityShards == 0 {
		return 1, 0
	}
	return dataShards, parityShards
}

func (s *Server) encodeChunkShards(chunkData []byte, dataShards, parityShards int) ([]shardPayload, error) {
	if dataShards == 1 && parityShards == 0 {
		return []shardPayload{{chunkID: "", shardIndex: 0, data: chunkData}}, nil
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	shards, err := enc.Split(chunkData)
	if err != nil {
		return nil, err
	}
	if err := enc.Encode(shards); err != nil {
		return nil, err
	}
	out := make([]shardPayload, 0, len(shards))
	for i, shard := range shards {
		out = append(out, shardPayload{
			chunkID:    "",
			shardIndex: i,
			data:       append([]byte(nil), shard...),
		})
	}
	return out, nil
}

func (s *Server) chunkShards(chunkRef model.ChunkRef) []model.ShardRef {
	if len(chunkRef.Shards) > 0 {
		return chunkRef.Shards
	}
	if chunkRef.NodeID != "" && chunkRef.ChunkID != "" {
		return []model.ShardRef{{
			ChunkID:    chunkRef.ChunkID,
			NodeID:     chunkRef.NodeID,
			ShardIndex: 0,
		}}
	}
	return nil
}

func (s *Server) readChunkPayload(
	ctx context.Context,
	nodeMap map[string]string,
	chunkRef model.ChunkRef,
) ([]byte, error) {
	shards := s.chunkShards(chunkRef)
	if len(shards) == 0 {
		return nil, fmt.Errorf("chunk has no shards")
	}
	dataShards := chunkRef.DataShards
	if dataShards < 1 {
		dataShards = 1
	}
	parityShards := chunkRef.ParityShards
	if parityShards < 0 {
		parityShards = 0
	}

	totalShards := len(shards)
	if totalShards < dataShards {
		return nil, fmt.Errorf("invalid shard layout")
	}
	if parityShards == 0 {
		parityShards = totalShards - dataShards
	}
	ordered := make([][]byte, totalShards)
	shardLen := 0
	type nodeBatchResult struct {
		chunks map[string][]byte
		err    error
	}
	idsByNode := make(map[string]map[string]struct{})
	for _, shard := range shards {
		if idsByNode[shard.NodeID] == nil {
			idsByNode[shard.NodeID] = make(map[string]struct{})
		}
		idsByNode[shard.NodeID][shard.ChunkID] = struct{}{}
	}
	results := make(chan nodeBatchResult, len(idsByNode))
	var wg sync.WaitGroup
	for _, shard := range shards {
		if shard.ShardIndex < 0 || shard.ShardIndex >= totalShards {
			return nil, fmt.Errorf("invalid shard index %d", shard.ShardIndex)
		}
	}
	for nodeID, idSet := range idsByNode {
		baseURL, ok := nodeMap[nodeID]
		if !ok {
			continue
		}
		chunkIDs := make([]string, 0, len(idSet))
		for chunkID := range idSet {
			chunkIDs = append(chunkIDs, chunkID)
		}
		wg.Add(1)
		go func(baseURL string, chunkIDs []string) {
			defer wg.Done()
			chunks, err := s.chunk.BatchGetChunks(ctx, baseURL, chunkIDs)
			if err != nil {
				results <- nodeBatchResult{err: err}
				return
			}
			results <- nodeBatchResult{chunks: chunks}
		}(baseURL, chunkIDs)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	payloadByChunkID := make(map[string][]byte, len(shards))
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for chunkID, payload := range result.chunks {
			payloadByChunkID[chunkID] = payload
		}
	}
	available := 0
	for _, shard := range shards {
		payload, ok := payloadByChunkID[shard.ChunkID]
		if !ok {
			continue
		}
		ordered[shard.ShardIndex] = payload
		if len(payload) > shardLen {
			shardLen = len(payload)
		}
		available++
	}
	if available < dataShards {
		return nil, fmt.Errorf("insufficient shards to reconstruct chunk")
	}
	for i := 0; i < totalShards; i++ {
		if ordered[i] == nil {
			ordered[i] = make([]byte, shardLen)
			continue
		}
		if len(ordered[i]) < shardLen {
			padded := make([]byte, shardLen)
			copy(padded, ordered[i])
			ordered[i] = padded
		}
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	if err := enc.Reconstruct(ordered); err != nil {
		return nil, err
	}

	var rebuilt bytes.Buffer
	for i := 0; i < dataShards; i++ {
		rebuilt.Write(ordered[i])
	}
	out := rebuilt.Bytes()
	if uint64(len(out)) > chunkRef.Size {
		out = out[:chunkRef.Size]
	}
	return out, nil
}
