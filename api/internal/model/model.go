package model

type ChunkRef struct {
	ChunkID string `json:"chunk_id"`
	NodeID  string `json:"node_id"`
	Offset  uint64 `json:"offset"`
	Size    uint64 `json:"size"`
}

type PutObjectRequest struct {
	Size     uint64     `json:"size"`
	ETag     string     `json:"etag"`
	Manifest []ChunkRef `json:"manifest"`
}

type ObjectMeta struct {
	Size     uint64     `json:"size"`
	ETag     string     `json:"etag"`
	Manifest []ChunkRef `json:"manifest"`
}

type ListBucketResponse struct {
	Keys       []string `json:"keys"`
	NextCursor *string  `json:"next_cursor"`
}

type ChunkInUseResult struct {
	InUse bool `json:"in_use"`
}

type ChunkerNode struct {
	NodeID  string `json:"node_id"`
	BaseURL string `json:"base_url"`
	Healthy bool   `json:"healthy"`
}
