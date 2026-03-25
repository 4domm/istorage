package errors

import (
	"encoding/json"
	"net/http"
)

type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func Validation(message string) error {
	return &APIError{Status: http.StatusBadRequest, Message: message}
}

func NotFound(message string) error {
	return &APIError{Status: http.StatusNotFound, Message: message}
}

func Internal(message string) error {
	return &APIError{Status: http.StatusInternalServerError, Message: message}
}

func Write(w http.ResponseWriter, err error) {
	apiErr, ok := err.(*APIError)
	if !ok {
		apiErr = &APIError{Status: http.StatusInternalServerError, Message: "internal error"}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.Status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": apiErr.Message})
}
