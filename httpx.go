package images

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

func WriteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

func DecodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

func ParseUintHeader(r *http.Request, name string, bits int) (uint64, error) {
	raw := strings.TrimSpace(r.Header.Get(name))
	if raw == "" {
		return 0, errors.New("missing header " + name)
	}
	return strconv.ParseUint(raw, 10, bits)
}

func CopyHeaders(dst, src http.Header, names ...string) {
	for _, name := range names {
		values := src.Values(name)
		if len(values) == 0 {
			continue
		}
		dst.Del(name)
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}
