package images

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
)

type ImageMetadata struct {
	ContentType string `json:"content_type"`
	Checksum    string `json:"checksum"`
	Size        uint64 `json:"size"`
}

func DetectImageMetadata(body []byte, hintedType string) ImageMetadata {
	metadata := ImageMetadata{
		Size: uint64(len(body)),
	}
	sum := sha256.Sum256(body)
	metadata.Checksum = hex.EncodeToString(sum[:])

	contentType := hintedType
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = http.DetectContentType(body)
	}
	metadata.ContentType = contentType
	return metadata
}
