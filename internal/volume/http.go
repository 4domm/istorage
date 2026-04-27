package volume

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/4domm/images/internal/common"
)

func NewHandler(store *Store, cfg Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/internal/packs/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req common.CreatePackRequest
		if err := common.DecodeJSON(r, &req); err != nil {
			common.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := store.CreateVolume(req.PackID, req.MaxPackBytes); err != nil {
			common.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		common.WriteJSON(w, http.StatusOK, map[string]string{"status": "created"})
	})
	mux.HandleFunc("/internal/packs/", func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.TrimPrefix(r.URL.Path, "/internal/packs/")
		parts := strings.Split(trimmed, "/")
		if len(parts) < 2 {
			http.NotFound(w, r)
			return
		}
		packID, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			common.WriteError(w, http.StatusBadRequest, "invalid pack id")
			return
		}
		action := parts[1]
		switch action {
		case "write-primary":
			handleBinaryWrite(w, r, func(req common.EntryWriteRequest, body []byte) error {
				return store.WritePrimary(uint32(packID), req, body)
			})
		case "replicate":
			handleBinaryWrite(w, r, func(req common.EntryWriteRequest, body []byte) error {
				return store.Replicate(uint32(packID), req, body)
			})
		case "delete":
			var req common.EntryDeleteRequest
			if err := common.DecodeJSON(r, &req); err != nil {
				common.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := store.DeleteReplica(uint32(packID), req); err != nil {
				common.WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "delete-primary":
			var req common.EntryDeleteRequest
			if err := common.DecodeJSON(r, &req); err != nil {
				common.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := store.DeletePrimary(uint32(packID), req); err != nil {
				common.WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "compact":
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			if err := store.Compact(uint32(packID)); err != nil {
				common.WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			common.WriteJSON(w, http.StatusOK, map[string]string{"status": "compacted"})
		case "repair":
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			var replicas []common.Replica
			if err := json.NewDecoder(r.Body).Decode(&replicas); err != nil {
				common.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := store.RepairVolume(uint32(packID), replicas); err != nil {
				common.WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			common.WriteJSON(w, http.StatusOK, map[string]string{"status": "repaired"})
		case "entries":
			if len(parts) != 3 {
				http.NotFound(w, r)
				return
			}
			entryID, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				common.WriteError(w, http.StatusBadRequest, "invalid entry id")
				return
			}
			guard, err := strconv.ParseUint(r.URL.Query().Get("guard"), 10, 32)
			if err != nil {
				common.WriteError(w, http.StatusBadRequest, "invalid guard")
				return
			}
			item, reader, err := store.Read(uint32(packID), entryID, uint32(guard))
			if err != nil {
				if err == os.ErrNotExist {
					common.WriteError(w, http.StatusNotFound, "entry not found")
					return
				}
				common.WriteError(w, http.StatusInternalServerError, err.Error())
				return
			}
			defer reader.Close()
			w.Header().Set("Content-Type", item.Metadata.ContentType)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", item.Size))
			w.Header().Set("ETag", `"`+item.Metadata.Checksum+`"`)
			http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(mustReadAll(reader)))
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

func handleBinaryWrite(w http.ResponseWriter, r *http.Request, fn func(req common.EntryWriteRequest, body []byte) error) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	meta := r.Header.Get("X-Entry-Meta")
	if meta == "" {
		common.WriteError(w, http.StatusBadRequest, "missing X-Entry-Meta")
		return
	}
	var req common.EntryWriteRequest
	if err := json.Unmarshal([]byte(meta), &req); err != nil {
		common.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		common.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := fn(req, body); err != nil {
		common.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func postBinary(client *http.Client, baseURL, path string, req common.EntryWriteRequest, body []byte) error {
	meta, err := json.Marshal(req)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("X-Entry-Meta", string(meta))
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}

func mustReadAll(r io.Reader) []byte {
	body, _ := io.ReadAll(r)
	return body
}
