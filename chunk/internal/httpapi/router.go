package httpapi

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"istorage/chunk/internal/store"
)

type Server struct {
	store        *store.Store
	maxBodyBytes int64
}

type batchRequest struct {
	ChunkIDs []string `json:"chunk_ids"`
}

type batchExistsResponse struct {
	Missing []string `json:"missing"`
}

func New(store *store.Store, maxBodyBytes int64) *Server {
	return &Server{
		store:        store,
		maxBodyBytes: maxBodyBytes,
	}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/health":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/chunks/batch-put":
			s.handleBatchPut(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/chunks/batch-get":
			s.handleBatchGet(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/chunks/batch-exists":
			s.handleBatchExists(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/chunks/batch-delete":
			s.handleBatchDelete(w, r)
		case strings.HasPrefix(r.URL.Path, "/chunks/"):
			chunkID := strings.TrimPrefix(r.URL.Path, "/chunks/")
			switch r.Method {
			case http.MethodPut:
				s.handlePut(w, r, chunkID)
			case http.MethodGet:
				s.handleGet(w, r, chunkID)
			case http.MethodDelete:
				s.handleDelete(w, r, chunkID)
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, chunkID string) {
	if err := validateChunkID(chunkID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	body := http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	data, err := ioReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.Put(chunkID, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleGet(w http.ResponseWriter, _ *http.Request, chunkID string) {
	if err := validateChunkID(chunkID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data, err := s.store.Get(chunkID)
	if err != nil {
		if err == store.ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleDelete(w http.ResponseWriter, _ *http.Request, chunkID string) {
	if err := validateChunkID(chunkID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.Delete(chunkID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleBatchPut(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	count, err := readU32(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if count == 0 || count > 4096 {
		http.Error(w, "invalid batch item count", http.StatusBadRequest)
		return
	}

	items := make([]store.ChunkWrite, 0, int(count))
	for i := uint32(0); i < count; i++ {
		idLen, err := readU32(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if idLen == 0 || idLen > 1024 {
			http.Error(w, "invalid chunk_id length", http.StatusBadRequest)
			return
		}
		idBuf := make([]byte, idLen)
		if _, err := io.ReadFull(r.Body, idBuf); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		chunkID := string(idBuf)
		if err := validateChunkID(chunkID); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		dataLen, err := readU64(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if dataLen > uint64(s.maxBodyBytes) {
			http.Error(w, "chunk payload exceeds max_body_bytes", http.StatusBadRequest)
			return
		}
		data := make([]byte, int(dataLen))
		if _, err := io.ReadFull(r.Body, data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		items = append(items, store.ChunkWrite{ChunkID: chunkID, Data: data})
	}

	if err := s.store.BatchPut(items); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleBatchGet(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBatchRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	if err := writeU32(w, uint32(len(req.ChunkIDs))); err != nil {
		return
	}

	for _, chunkID := range req.ChunkIDs {
		data, err := s.store.Get(chunkID)
		if err != nil {
			return
		}
		if err := writeU32(w, uint32(len(chunkID))); err != nil {
			return
		}
		if _, err := io.WriteString(w, chunkID); err != nil {
			return
		}
		if err := writeU64(w, uint64(len(data))); err != nil {
			return
		}
		if _, err := w.Write(data); err != nil {
			return
		}
	}
}

func (s *Server) handleBatchDelete(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBatchRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.BatchDelete(req.ChunkIDs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleBatchExists(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBatchRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	missing, err := s.store.BatchMissing(req.ChunkIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(batchExistsResponse{Missing: missing})
}

func decodeBatchRequest(r *http.Request) (batchRequest, error) {
	defer r.Body.Close()
	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return batchRequest{}, err
	}
	if len(req.ChunkIDs) == 0 {
		return batchRequest{}, errInvalid("chunk_ids must be non-empty")
	}
	for _, chunkID := range req.ChunkIDs {
		if err := validateChunkID(chunkID); err != nil {
			return batchRequest{}, err
		}
	}
	return req, nil
}

func validateChunkID(chunkID string) error {
	if chunkID == "" || strings.Contains(chunkID, "/") {
		return errInvalid("invalid chunk_id")
	}
	return nil
}

type invalidError string

func (e invalidError) Error() string {
	return string(e)
}

func errInvalid(message string) error {
	return invalidError(message)
}

func ioReadAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("invalid batch framing")
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("invalid batch framing")
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func writeU32(w io.Writer, value uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}

func writeU64(w io.Writer, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}
