package images

type Replica struct {
	ServerID string `json:"server_id"`
	URL      string `json:"url"`
}

type PackState string

const (
	PackStateWritable   PackState = "writable"
	PackStateReadonly   PackState = "readonly"
	PackStateCompacting PackState = "compacting"
	PackStateDead       PackState = "dead"
)

type AllocateRequest struct {
	Size uint64 `json:"size"`
}

type AllocateResponse struct {
	BlobID   string    `json:"blob_id"`
	PackID   uint32    `json:"pack_id"`
	EntryID  uint64    `json:"entry_id"`
	Guard    uint32    `json:"guard"`
	Primary  Replica   `json:"primary"`
	Replicas []Replica `json:"replicas"`
}

type LookupResponse struct {
	PackID   uint32    `json:"pack_id"`
	State    PackState `json:"state"`
	Primary  Replica   `json:"primary"`
	Replicas []Replica `json:"replicas"`
}

type CreatePackRequest struct {
	PackID       uint32 `json:"pack_id"`
	MaxPackBytes int64  `json:"max_pack_bytes"`
}

type HeartbeatPack struct {
	PackID uint32    `json:"pack_id"`
	State  PackState `json:"state"`
	Size   int64     `json:"size"`
}

type HeartbeatRequest struct {
	ServerID     string          `json:"server_id"`
	URL          string          `json:"url"`
	FreeBytes    int64           `json:"free_bytes"`
	MaxPackBytes int64           `json:"max_pack_bytes"`
	Packs        []HeartbeatPack `json:"packs"`
}

type EntryWriteRequest struct {
	EntryID  uint64        `json:"entry_id"`
	Guard    uint32        `json:"guard"`
	Metadata ImageMetadata `json:"metadata"`
	Replicas []Replica     `json:"replicas,omitempty"`
}

type EntryDeleteRequest struct {
	EntryID  uint64    `json:"entry_id"`
	Guard    uint32    `json:"guard"`
	Replicas []Replica `json:"replicas,omitempty"`
}
