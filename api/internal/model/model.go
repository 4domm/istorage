package model

type ShardRef struct {
	ChunkID    string `json:"chunk_id"`
	NodeID     string `json:"node_id"`
	ShardIndex int    `json:"shard_index"`
}

type ChunkRef struct {
	Offset       uint64     `json:"offset"`
	Size         uint64     `json:"size"`
	DataShards   int        `json:"data_shards"`
	ParityShards int        `json:"parity_shards"`
	Shards       []ShardRef `json:"shards"`
	ChunkID      string     `json:"chunk_id,omitempty"`
	NodeID       string     `json:"node_id,omitempty"`
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

type ChunkerNode struct {
	NodeID  string `json:"node_id"`
	Zone    string `json:"zone"`
	BaseURL string `json:"base_url"`
	Healthy bool   `json:"healthy"`
}
