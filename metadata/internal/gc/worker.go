package gc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sstorage/metadata/internal/chunk"
	"sstorage/metadata/internal/chunkers"
	"sstorage/metadata/internal/service"
)

const (
	deleteAttempts    = 4
	emptyPollDelay    = 300 * time.Millisecond
	errorPollDelay    = time.Second
	claimLeaseSeconds = 30
)

func StartWorker(
	ctx context.Context,
	logger *slog.Logger,
	svc *service.Service,
	registry *chunkers.Registry,
	batchSize int,
) {
	client := chunk.New()
	workerID := gcWorkerID()
	if batchSize < 1 {
		batchSize = 1
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			tasks, err := svc.GCNextBatch(ctx, batchSize, workerID, claimLeaseSeconds*time.Second)
			if err != nil {
				logger.Warn("gc poll failed", "error", err)
				time.Sleep(errorPollDelay)
				continue
			}
			if len(tasks) == 0 {
				time.Sleep(emptyPollDelay)
				continue
			}

			nodes := registry.List(false)
			nodeMap := make(map[string]string, len(nodes))
			for _, node := range nodes {
				nodeMap[node.NodeID] = node.BaseURL
			}

			grouped := make(map[string]struct {
				baseURL string
				seqs    []uint64
				ids     []string
			})
			for _, task := range tasks {
				baseURL, ok := nodeMap[task.NodeID]
				if !ok {
					logger.Warn("gc task references unknown node", "node_id", task.NodeID, "seq", task.Seq)
					continue
				}
				group := grouped[task.NodeID]
				group.baseURL = baseURL
				group.seqs = append(group.seqs, task.Seq)
				group.ids = append(group.ids, task.ChunkID)
				grouped[task.NodeID] = group
			}

			var acked []uint64
			for nodeID, group := range grouped {
				chunkIDs := dedupe(group.ids)
				deleted := false
				for attempt := 1; attempt <= deleteAttempts; attempt++ {
					err := client.BatchDeleteChunks(ctx, group.baseURL, chunkIDs)
					if err == nil {
						deleted = true
						break
					}
					if attempt < deleteAttempts {
						logger.Warn("gc batch delete attempt failed", "node_id", nodeID, "attempt", attempt, "error", err)
						time.Sleep(time.Second)
						continue
					}
					logger.Error("gc batch delete permanently failed", "node_id", nodeID, "attempts", attempt, "error", err)
				}
				if deleted {
					acked = append(acked, group.seqs...)
				}
			}

			if len(acked) == 0 {
				time.Sleep(errorPollDelay)
				continue
			}

			if ackedCount, err := svc.GCAckBatch(ctx, workerID, acked); err != nil {
				logger.Warn("gc batch ack failed", "error", err)
			} else if ackedCount != uint64(len(acked)) {
				logger.Warn("gc batch ack mismatch", "expected", len(acked), "acked", ackedCount)
			}
		}
	}()
}

func dedupe(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func gcWorkerID() string {
	return fmt.Sprintf("metadata-%d", time.Now().UnixNano())
}
