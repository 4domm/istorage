package fdbstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"istorage/metadata/internal/model"
)

var ErrNotFound = errors.New("not found")

var (
	objectsPrefix       = []byte("objects\x00")
	bucketObjectsPrefix = []byte("bucket_objects\x00")
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

		oldChunks := make([]chunkKey, 0)
		if len(oldRaw) > 0 {
			var oldMeta model.ObjectMeta
			if err := json.Unmarshal(oldRaw, &oldMeta); err != nil {
				return nil, err
			}
			oldChunks = manifestToChunkKeys(oldMeta.Manifest)
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
		if err := enqueueGCTasks(tr, oldChunks); err != nil {
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
		if err := enqueueGCTasks(tr, manifestToChunkKeys(meta.Manifest)); err != nil {
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

func (s *Store) GCNextBatch(ctx context.Context, limit int, owner string, lease time.Duration) ([]model.GcTask, error) {
	if limit < 1 {
		return []model.GcTask{}, nil
	}
	value, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		now := time.Now().UnixMilli()
		leaseUntil := now + lease.Milliseconds()

		tasks := make([]model.GcTask, 0, limit)
		begin := append([]byte{}, gcQueuePrefix...)
		end := prefixEnd(gcQueuePrefix)
		pageLimit := limit * 4
		if pageLimit < 64 {
			pageLimit = 64
		}

		for len(tasks) < limit {
			rows, err := tr.GetRange(fdb.KeyRange{
				Begin: fdb.Key(begin),
				End:   fdb.Key(end),
			}, fdb.RangeOptions{Limit: pageLimit}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				break
			}
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

			lastKey := rows[len(rows)-1].Key
			begin = append(append([]byte{}, lastKey...), 0x00)
			if len(rows) < pageLimit {
				break
			}
		}
		return tasks, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]model.GcTask), nil
}

func (s *Store) GCAckBatch(ctx context.Context, owner string, seqs []uint64) (uint64, error) {
	if len(seqs) == 0 {
		return 0, nil
	}
	value, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		minSeq := seqs[0]
		maxSeq := seqs[0]
		seqSet := make(map[uint64]struct{}, len(seqs))
		for _, seq := range seqs {
			seqSet[seq] = struct{}{}
			if seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}

		endKey := gcQueueKey(maxSeq + 1)
		if maxSeq == ^uint64(0) {
			endKey = fdb.Key(prefixEnd(gcQueuePrefix))
		}
		rows, err := tr.GetRange(fdb.KeyRange{
			Begin: gcQueueKey(minSeq),
			End:   endKey,
		}, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		var acked uint64
		for _, row := range rows {
			var entry gcEntry
			if err := json.Unmarshal(row.Value, &entry); err != nil {
				return nil, err
			}
			if _, ok := seqSet[entry.Seq]; !ok {
				continue
			}
			if entry.ClaimedBy != owner {
				continue
			}
			tr.Clear(row.Key)
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

func manifestToChunkKeys(manifest []model.ChunkRef) []chunkKey {
	out := make([]chunkKey, 0, len(manifest))
	for _, chunk := range manifest {
		if len(chunk.Shards) == 0 {
			if chunk.NodeID != "" && chunk.ChunkID != "" {
				out = append(out, chunkKey{nodeID: chunk.NodeID, chunkID: chunk.ChunkID})
			}
			continue
		}
		for _, shard := range chunk.Shards {
			out = append(out, chunkKey{nodeID: shard.NodeID, chunkID: shard.ChunkID})
		}
	}
	return out
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
