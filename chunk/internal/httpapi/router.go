package httpapi

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"sstorage/chunk/internal/store"
)

type Server struct {
	store        *store.Store
	maxBodyBytes int64
}

type batchRequest struct {
	ChunkIDs []string `json:"chunk_ids"`
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
		case r.Method == http.MethodPost && r.URL.Path == "/chunks/batch-get":
			s.handleBatchGet(w, r)
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
	existed, err := s.store.PutIfAbsent(chunkID, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if existed {
		w.WriteHeader(http.StatusOK)
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

func (s *Server) handleBatchGet(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBatchRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	framed := make([]byte, 4)
	binary.BigEndian.PutUint32(framed[:4], uint32(len(req.ChunkIDs)))
	for _, chunkID := range req.ChunkIDs {
		data, err := s.store.Get(chunkID)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "batch chunk not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		idLen := make([]byte, 4)
		binary.BigEndian.PutUint32(idLen, uint32(len(chunkID)))
		size := make([]byte, 8)
		binary.BigEndian.PutUint64(size, uint64(len(data)))
		framed = append(framed, idLen...)
		framed = append(framed, []byte(chunkID)...)
		framed = append(framed, size...)
		framed = append(framed, data...)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(framed)
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
