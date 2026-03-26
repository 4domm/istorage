package fdbstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"sstorage/metadata/internal/model"
)

var ErrNotFound = errors.New("not found")

var (
	objectsPrefix       = []byte("objects\x00")
	bucketObjectsPrefix = []byte("bucket_objects\x00")
	refcountsPrefix     = []byte("refcounts\x00")
	gcQueuePrefix       = []byte("gc_queue\x00")
	gcSeqKey            = fdb.Key("meta\x00gc_seq")
)

type Store struct {
	db fdb.Database
}

type gcEntry struct {
	Seq            uint64 `json:"seq"`
	NodeID         string `json:"node_id"`
	ChunkID        string `json:"chunk_id"`
	ClaimedBy      string `json:"claimed_by,omitempty"`
	ClaimedUntilMS int64  `json:"claimed_until_ms,omitempty"`
}

type chunkKey struct {
	nodeID  string
	chunkID string
}

func Open(clusterFile string) (*Store, error) {
	var (
		db  fdb.Database
		err error
	)
	if clusterFile != "" {
		db, err = fdb.Open(clusterFile, []byte("DB"))
	} else {
		db, err = fdb.OpenDefault()
	}
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) PutObject(ctx context.Context, bucket, key string, body model.PutObjectRequest) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		objectKey := objectMetaKey(bucket, key)
		oldRaw, err := tr.Get(objectKey).Get()
		if err != nil {
			return nil, err
		}

		oldCounts := map[chunkKey]uint64{}
		if len(oldRaw) > 0 {
			var oldMeta model.ObjectMeta
			if err := json.Unmarshal(oldRaw, &oldMeta); err != nil {
				return nil, fmt.Errorf("decode old object: %w", err)
			}
			oldCounts = manifestCounts(oldMeta.Manifest)
		}

		newMeta := model.ObjectMeta{
			Size:     body.Size,
			ETag:     body.ETag,
			Manifest: body.Manifest,
		}
		encoded, err := json.Marshal(newMeta)
		if err != nil {
			return nil, err
		}

		tr.Set(objectKey, encoded)
		tr.Set(bucketObjectKey(bucket, key), []byte{1})

		deltas := diffManifestCounts(oldCounts, manifestCounts(body.Manifest))
		orphanChunks, err := applyRefcountDeltas(tr, deltas)
		if err != nil {
			return nil, err
		}
		if err := enqueueGCTasks(tr, orphanChunks); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (*model.ObjectMeta, error) {
	value, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		raw, err := tr.Get(objectMetaKey(bucket, key)).Get()
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, nil
		}
		var meta model.ObjectMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, err
		}
		return &meta, nil
	})
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	return value.(*model.ObjectMeta), nil
}

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) (bool, error) {
	value, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		raw, err := tr.Get(objectMetaKey(bucket, key)).Get()
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return false, nil
		}

		var meta model.ObjectMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, err
		}

		tr.Clear(objectMetaKey(bucket, key))
		tr.Clear(bucketObjectKey(bucket, key))

		deltas := map[chunkKey]int64{}
		for chunk, count := range manifestCounts(meta.Manifest) {
			deltas[chunk] = -int64(count)
		}
		orphanChunks, err := applyRefcountDeltas(tr, deltas)
		if err != nil {
			return nil, err
		}
		if err := enqueueGCTasks(tr, orphanChunks); err != nil {
			return nil, err
		}
		return true, nil
	})
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}

func (s *Store) ListBucketKeys(ctx context.Context, bucket string, limit int, cursor string) ([]string, *string, error) {
	value, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		begin := bucketListingPrefix(bucket)
		if cursor != "" {
			begin = bucketObjectKey(bucket, cursor)
			begin = append(append([]byte{}, begin...), 0x00)
		}
		rr := tr.GetRange(fdb.KeyRange{
			Begin: fdb.Key(begin),
			End:   fdb.Key(prefixEnd(bucketListingPrefix(bucket))),
		}, fdb.RangeOptions{Limit: limit + 1})
		rows, err := rr.GetSliceWithError()
		if err != nil {
			return nil, err
		}

		keys := make([]string, 0, min(limit, len(rows)))
		for i, kv := range rows {
			if i == limit {
				break
			}
			keys = append(keys, decodeBucketObjectKey(bucket, kv.Key))
		}
		var nextCursor *string
		if len(rows) > limit && len(keys) > 0 {
			cursor := keys[len(keys)-1]
			nextCursor = &cursor
		}
		return listBucketResult{keys: keys, nextCursor: nextCursor}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	result := value.(listBucketResult)
	return result.keys, result.nextCursor, nil
}

func (s *Store) ChunkInUse(ctx context.Context, nodeID, chunkID string) (bool, error) {
	value, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		raw, err := tr.Get(refcountKey(nodeID, chunkID)).Get()
		if err != nil {
			return nil, err
		}
		return len(raw) > 0 && decodeUint64(raw) > 0, nil
	})
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}

func (s *Store) GCNextBatch(ctx context.Context, limit int, owner string, lease time.Duration) ([]model.GcTask, error) {
	value, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		now := time.Now().UnixMilli()
		leaseUntil := now + lease.Milliseconds()
		gcRange, err := fdb.PrefixRange(gcQueuePrefix)
		if err != nil {
			return nil, err
		}
		rows, err := tr.GetRange(gcRange, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		tasks := make([]model.GcTask, 0, limit)
		for _, row := range rows {
			if len(tasks) == limit {
				break
			}
			var entry gcEntry
			if err := json.Unmarshal(row.Value, &entry); err != nil {
				return nil, err
			}
			if entry.ClaimedBy != "" && entry.ClaimedBy != owner && entry.ClaimedUntilMS >= now {
				continue
			}

			entry.ClaimedBy = owner
			entry.ClaimedUntilMS = leaseUntil
			encoded, err := json.Marshal(entry)
			if err != nil {
				return nil, err
			}
			tr.Set(row.Key, encoded)
			tasks = append(tasks, model.GcTask{
				Seq:     entry.Seq,
				ChunkID: entry.ChunkID,
				NodeID:  entry.NodeID,
			})
		}
		return tasks, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]model.GcTask), nil
}

func (s *Store) GCAckBatch(ctx context.Context, owner string, seqs []uint64) (uint64, error) {
	value, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var acked uint64
		for _, seq := range seqs {
			key := gcQueueKey(seq)
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(raw) == 0 {
				continue
			}
			var entry gcEntry
			if err := json.Unmarshal(raw, &entry); err != nil {
				return nil, err
			}
			if entry.ClaimedBy != owner {
				continue
			}
			tr.Clear(key)
			acked++
		}
		return acked, nil
	})
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

type listBucketResult struct {
	keys       []string
	nextCursor *string
}

func manifestCounts(manifest []model.ChunkRef) map[chunkKey]uint64 {
	counts := make(map[chunkKey]uint64, len(manifest))
	for _, chunk := range manifest {
		counts[chunkKey{nodeID: chunk.NodeID, chunkID: chunk.ChunkID}]++
	}
	return counts
}

func diffManifestCounts(oldCounts, newCounts map[chunkKey]uint64) map[chunkKey]int64 {
	deltas := make(map[chunkKey]int64, len(oldCounts)+len(newCounts))
	for chunk, count := range oldCounts {
		deltas[chunk] -= int64(count)
	}
	for chunk, count := range newCounts {
		deltas[chunk] += int64(count)
	}
	return deltas
}

func applyRefcountDeltas(tr fdb.Transaction, deltas map[chunkKey]int64) ([]chunkKey, error) {
	var orphanChunks []chunkKey
	for chunk, delta := range deltas {
		if delta == 0 {
			continue
		}
		key := refcountKey(chunk.nodeID, chunk.chunkID)
		currentRaw, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		current := int64(decodeUint64(currentRaw))
		next := current + delta
		if next <= 0 {
			tr.Clear(key)
			if current > 0 {
				orphanChunks = append(orphanChunks, chunk)
			}
			continue
		}
		tr.Set(key, encodeUint64(uint64(next)))
	}
	return orphanChunks, nil
}

func enqueueGCTasks(tr fdb.Transaction, chunks []chunkKey) error {
	if len(chunks) == 0 {
		return nil
	}
	nextSeq, err := nextGCSeq(tr, uint64(len(chunks)))
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		entry := gcEntry{
			Seq:     nextSeq,
			NodeID:  chunk.nodeID,
			ChunkID: chunk.chunkID,
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		tr.Set(gcQueueKey(nextSeq), encoded)
		nextSeq++
	}
	return nil
}

func nextGCSeq(tr fdb.Transaction, reserve uint64) (uint64, error) {
	raw, err := tr.Get(gcSeqKey).Get()
	if err != nil {
		return 0, err
	}
	current := decodeUint64(raw)
	next := current + reserve
	tr.Set(gcSeqKey, encodeUint64(next))
	return current + 1, nil
}

func objectMetaKey(bucket, key string) fdb.Key {
	return fdb.Key(appendKey(objectsPrefix, bucket, key))
}

func bucketListingPrefix(bucket string) []byte {
	return appendKey(bucketObjectsPrefix, bucket)
}

func bucketObjectKey(bucket, key string) fdb.Key {
	return fdb.Key(appendKey(bucketObjectsPrefix, bucket, key))
}

func refcountKey(nodeID, chunkID string) fdb.Key {
	return fdb.Key(appendKey(refcountsPrefix, nodeID, chunkID))
}

func gcQueueKey(seq uint64) fdb.Key {
	return fdb.Key(append(gcQueuePrefix, encodeUint64(seq)...))
}

func appendKey(prefix []byte, parts ...string) []byte {
	out := append([]byte{}, prefix...)
	for i, part := range parts {
		if i > 0 {
			out = append(out, 0x00)
		}
		out = append(out, []byte(part)...)
	}
	return out
}

func decodeBucketObjectKey(bucket string, key []byte) string {
	prefix := appendKey(bucketObjectsPrefix, bucket)
	if len(key) <= len(prefix)+1 {
		return ""
	}
	return string(key[len(prefix)+1:])
}

func prefixEnd(prefix []byte) []byte {
	return append(append([]byte{}, prefix...), 0xff)
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(raw []byte) uint64 {
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
