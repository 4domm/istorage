package coordinator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/4domm/images/internal/common"
)

type serverState struct {
	ServerID      string                `json:"server_id"`
	URL           string                `json:"url"`
	FreeBytes     int64                 `json:"free_bytes"`
	MaxPackBytes  int64                 `json:"max_pack_bytes"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
	Healthy       bool                  `json:"healthy"`
	Packs         map[uint32]serverPack `json:"packs"`
}

type serverPack struct {
	State common.PackState `json:"state"`
	Size  int64            `json:"size"`
}

type packState struct {
	PackID      uint32           `json:"pack_id"`
	State       common.PackState `json:"state"`
	Replicas    []common.Replica `json:"replicas"`
	Primary     common.Replica   `json:"primary"`
	NextEntryID uint64           `json:"next_entry_id"`
	MaxBytes    int64            `json:"max_bytes"`
	SizeBytes   int64            `json:"size_bytes"`
}

type Registry struct {
	mu          sync.Mutex
	cfg         Config
	client      *http.Client
	db          *badger.DB
	nextPack    uint32
	servers     map[string]*serverState
	packs       map[uint32]*packState
	objectCache *objectCache
}

func LoadRegistry(cfg Config) (*Registry, error) {
	if err := os.MkdirAll(filepath.Clean(cfg.DBPath), 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(cfg.DBPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	r := &Registry{
		cfg:         cfg,
		client:      &http.Client{Timeout: cfg.HTTPTimeout},
		db:          db,
		servers:     make(map[string]*serverState),
		packs:       make(map[uint32]*packState),
		objectCache: newObjectCache(cfg.ObjectCacheEntries),
	}
	if err := r.load(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if r.nextPack == 0 {
		r.nextPack = 1
	}
	return r, nil
}

func (r *Registry) Close() error {
	return r.db.Close()
}

func (r *Registry) load() error {
	return r.db.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(metaNextPackKey()); err == nil {
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &r.nextPack); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(serverPrefix()); iter.ValidForPrefix(serverPrefix()); iter.Next() {
			item := iter.Item()
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var server serverState
			if err := json.Unmarshal(raw, &server); err != nil {
				return err
			}
			serverCopy := server
			r.servers[server.ServerID] = &serverCopy
		}

		for iter.Seek(packPrefix()); iter.ValidForPrefix(packPrefix()); iter.Next() {
			item := iter.Item()
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var pack packState
			if err := json.Unmarshal(raw, &pack); err != nil {
				return err
			}
			packCopy := pack
			r.packs[pack.PackID] = &packCopy
		}
		return nil
	})
}

func (r *Registry) saveLocked() error {
	return r.db.Update(func(txn *badger.Txn) error {
		rawNext, err := json.Marshal(r.nextPack)
		if err != nil {
			return err
		}
		if err := txn.Set(metaNextPackKey(), rawNext); err != nil {
			return err
		}
		for serverID, server := range r.servers {
			raw, err := json.Marshal(server)
			if err != nil {
				return err
			}
			if err := txn.Set(serverKey(serverID), raw); err != nil {
				return err
			}
		}
		for packID, pack := range r.packs {
			raw, err := json.Marshal(pack)
			if err != nil {
				return err
			}
			if err := txn.Set(packKey(packID), raw); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Registry) Heartbeat(req common.HeartbeatRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.servers[req.ServerID]
	if !ok {
		state = &serverState{ServerID: req.ServerID}
		r.servers[req.ServerID] = state
	}
	state.URL = req.URL
	state.FreeBytes = req.FreeBytes
	state.MaxPackBytes = req.MaxPackBytes
	state.LastHeartbeat = time.Now().UTC()
	state.Healthy = true
	state.Packs = make(map[uint32]serverPack, len(req.Packs))
	for _, pack := range req.Packs {
		state.Packs[pack.PackID] = serverPack{State: pack.State, Size: pack.Size}
		if existing, ok := r.packs[pack.PackID]; ok {
			existing.SizeBytes = pack.Size
			if existing.State != common.PackStateCompacting {
				existing.State = pack.State
			}
		}
	}
	return r.saveLocked()
}

func (r *Registry) Allocate(ctx context.Context, size uint64) (common.AllocateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pack, err := r.findWritablePackLocked(size)
	if err != nil {
		return common.AllocateResponse{}, err
	}
	entryID := pack.NextEntryID
	pack.NextEntryID++
	pack.SizeBytes += int64(size)
	if pack.SizeBytes >= pack.MaxBytes {
		pack.State = common.PackStateReadonly
	}
	guard, err := randomUint32()
	if err != nil {
		return common.AllocateResponse{}, err
	}
	blobID := common.BlobID{PackID: pack.PackID, EntryID: entryID, Guard: guard}
	if err := r.saveLocked(); err != nil {
		return common.AllocateResponse{}, err
	}
	return common.AllocateResponse{
		BlobID:   blobID.String(),
		PackID:   pack.PackID,
		EntryID:  entryID,
		Guard:    guard,
		Primary:  pack.Primary,
		Replicas: append([]common.Replica(nil), pack.Replicas...),
	}, nil
}

func (r *Registry) Lookup(packID uint32) (common.LookupResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pack, ok := r.packs[packID]
	if !ok {
		return common.LookupResponse{}, fmt.Errorf("pack not found")
	}
	return common.LookupResponse{
		PackID:   packID,
		State:    pack.State,
		Primary:  pack.Primary,
		Replicas: append([]common.Replica(nil), pack.Replicas...),
	}, nil
}

func (r *Registry) findWritablePackLocked(size uint64) (*packState, error) {
	for _, pack := range r.packs {
		if pack.State == common.PackStateWritable && pack.SizeBytes+int64(size) <= pack.MaxBytes {
			return pack, nil
		}
	}
	return r.createPackLocked()
}

func (r *Registry) createPackLocked() (*packState, error) {
	healthy := make([]*serverState, 0, len(r.servers))
	for _, server := range r.servers {
		if server.Healthy {
			healthy = append(healthy, server)
		}
	}
	if len(healthy) < r.cfg.ReplicaCount {
		return nil, fmt.Errorf("need at least %d healthy storage nodes", r.cfg.ReplicaCount)
	}
	sort.Slice(healthy, func(i, j int) bool {
		if healthy[i].FreeBytes == healthy[j].FreeBytes {
			return healthy[i].ServerID < healthy[j].ServerID
		}
		return healthy[i].FreeBytes > healthy[j].FreeBytes
	})
	selected := healthy[:r.cfg.ReplicaCount]
	replicas := make([]common.Replica, 0, len(selected))
	for _, server := range selected {
		replicas = append(replicas, common.Replica{ServerID: server.ServerID, URL: server.URL})
	}
	packID := r.nextPack
	r.nextPack++
	for _, replica := range replicas {
		payload := common.CreatePackRequest{PackID: packID, MaxPackBytes: r.cfg.PackSizeBytes}
		if err := postJSON(r.client, ctxWithTimeout(), replica.URL+"/internal/packs/create", payload, nil); err != nil {
			return nil, fmt.Errorf("create pack on %s: %w", replica.ServerID, err)
		}
	}
	pack := &packState{
		PackID:      packID,
		State:       common.PackStateWritable,
		Replicas:    replicas,
		Primary:     replicas[0],
		NextEntryID: 1,
		MaxBytes:    r.cfg.PackSizeBytes,
	}
	r.packs[packID] = pack
	for _, server := range selected {
		if server.Packs == nil {
			server.Packs = map[uint32]serverPack{}
		}
		server.Packs[packID] = serverPack{State: common.PackStateWritable}
		server.FreeBytes -= r.cfg.PackSizeBytes
		if server.FreeBytes < 0 {
			server.FreeBytes = 0
		}
	}
	if err := r.saveLocked(); err != nil {
		return nil, err
	}
	return pack, nil
}

func (r *Registry) Status() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	packs := make([]*packState, 0, len(r.packs))
	for _, pack := range r.packs {
		copyPack := *pack
		packs = append(packs, &copyPack)
	}
	sort.Slice(packs, func(i, j int) bool { return packs[i].PackID < packs[j].PackID })
	return map[string]any{
		"next_pack_id": r.nextPack,
		"server_count": len(r.servers),
		"pack_count":   len(r.packs),
		"packs":        packs,
	}
}

func ctxWithTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return ctx
}

func randomUint32() (uint32, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(math.MaxUint32)))
	if err != nil {
		return 0, err
	}
	return uint32(n.Uint64() + 1), nil
}

func postJSON(client *http.Client, ctx context.Context, url string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func metaNextPackKey() []byte {
	return []byte("meta/next_pack_id")
}

func serverPrefix() []byte {
	return []byte("server/")
}

func serverKey(serverID string) []byte {
	return []byte("server/" + serverID)
}

func packPrefix() []byte {
	return []byte("pack/")
}

func packKey(packID uint32) []byte {
	return []byte(fmt.Sprintf("pack/%08x", packID))
}
