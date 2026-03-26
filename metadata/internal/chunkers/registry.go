package chunkers

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type Node struct {
	NodeID  string `json:"node_id"`
	BaseURL string `json:"base_url"`
	Healthy bool   `json:"healthy"`
}

type Registry struct {
	mu    sync.RWMutex
	nodes map[string]Node
}

func New(nodes []Node) *Registry {
	index := make(map[string]Node, len(nodes))
	for _, node := range nodes {
		index[node.NodeID] = node
	}
	return &Registry{nodes: index}
}

func (r *Registry) List(healthyOnly bool) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		if healthyOnly && !node.Healthy {
			continue
		}
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (r *Registry) SetHealth(nodeID string, healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeID]
	if !ok {
		return
	}
	node.Healthy = healthy
	r.nodes[nodeID] = node
}

func (r *Registry) PlaceChunks(chunkIDs []string, healthyOnly bool) (map[string]Node, error) {
	nodes := r.List(healthyOnly)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no chunker nodes available")
	}
	out := make(map[string]Node, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		best := nodes[0]
		bestScore := scoreNode(chunkID, best.NodeID)
		for _, node := range nodes[1:] {
			score := scoreNode(chunkID, node.NodeID)
			if score > bestScore {
				best = node
				bestScore = score
			}
		}
		out[chunkID] = best
	}
	return out, nil
}

func scoreNode(chunkID, nodeID string) uint64 {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", chunkID, nodeID)))
	return binary.BigEndian.Uint64(digest[:8])
}
