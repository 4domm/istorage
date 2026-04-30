package images

import (
	"container/list"
	"encoding/json"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type objectRecord struct {
	Bucket    string        `json:"bucket"`
	Key       string        `json:"key"`
	BlobID    string        `json:"blob_id"`
	Metadata  ImageMetadata `json:"metadata"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type objectCacheKey struct {
	bucket string
	key    string
}

type objectCacheValue struct {
	key    objectCacheKey
	record objectRecord
}

type objectCache struct {
	capacity int
	ll       *list.List
	items    map[objectCacheKey]*list.Element
}

func newObjectCache(capacity int) *objectCache {
	if capacity <= 0 {
		capacity = 1
	}
	return &objectCache{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[objectCacheKey]*list.Element, capacity),
	}
}

func (c *objectCache) Get(bucket, key string) (objectRecord, bool) {
	cacheKey := objectCacheKey{bucket: bucket, key: key}
	elem, ok := c.items[cacheKey]
	if !ok {
		return objectRecord{}, false
	}
	c.ll.MoveToFront(elem)
	return elem.Value.(objectCacheValue).record, true
}

func (c *objectCache) Put(record objectRecord) {
	cacheKey := objectCacheKey{bucket: record.Bucket, key: record.Key}
	if elem, ok := c.items[cacheKey]; ok {
		elem.Value = objectCacheValue{key: cacheKey, record: record}
		c.ll.MoveToFront(elem)
		return
	}
	elem := c.ll.PushFront(objectCacheValue{key: cacheKey, record: record})
	c.items[cacheKey] = elem
	if c.ll.Len() <= c.capacity {
		return
	}
	tail := c.ll.Back()
	if tail == nil {
		return
	}
	evicted := tail.Value.(objectCacheValue)
	delete(c.items, evicted.key)
	c.ll.Remove(tail)
}

func (c *objectCache) Delete(bucket, key string) {
	cacheKey := objectCacheKey{bucket: bucket, key: key}
	elem, ok := c.items[cacheKey]
	if !ok {
		return
	}
	delete(c.items, cacheKey)
	c.ll.Remove(elem)
}

func (r *Registry) OwnsBucket(bucket string) bool {
	if bucket == "" {
		return false
	}
	owner, ok := r.ShardOwner(bucket)
	if !ok {
		return false
	}
	return owner == r.cfg.ShardID
}

func (r *Registry) ShardOwner(bucket string) (int, bool) {
	if bucket == "" || r.cfg.ShardCount <= 0 {
		return 0, false
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(bucket))
	return int(hasher.Sum32() % uint32(r.cfg.ShardCount)), true
}

func (r *Registry) RedirectURL(bucket, requestURI string) (string, bool) {
	owner, ok := r.ShardOwner(bucket)
	if !ok || owner == r.cfg.ShardID {
		return "", false
	}
	if owner < 0 || owner >= len(r.cfg.ShardURLs) {
		return "", false
	}
	base := strings.TrimRight(r.cfg.ShardURLs[owner], "/")
	if base == "" {
		return "", false
	}
	return base + requestURI, true
}

func (r *Registry) PutObject(bucket, key string, record objectRecord) (*objectRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var previous *objectRecord
	err := r.db.Update(func(txn *badger.Txn) error {
		if item, err := txn.Get(objectKey(bucket, key)); err == nil {
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var old objectRecord
			if err := json.Unmarshal(raw, &old); err != nil {
				return err
			}
			previous = &old
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return txn.Set(objectKey(bucket, key), raw)
	})
	if err != nil {
		return nil, err
	}
	r.objectCache.Put(record)
	return previous, nil
}

func (r *Registry) GetObject(bucket, key string) (*objectRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if record, ok := r.objectCache.Get(bucket, key); ok {
		copyRecord := record
		return &copyRecord, nil
	}

	var out objectRecord
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectKey(bucket, key))
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, &out)
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	r.objectCache.Put(out)
	return &out, nil
}

func (r *Registry) DeleteObject(bucket, key string) (*objectRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var previous *objectRecord
	err := r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(objectKey(bucket, key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		var old objectRecord
		if err := json.Unmarshal(raw, &old); err != nil {
			return err
		}
		previous = &old
		return txn.Delete(objectKey(bucket, key))
	})
	if err != nil {
		return nil, err
	}
	r.objectCache.Delete(bucket, key)
	return previous, nil
}

func (r *Registry) ListObjects(bucket, prefix string, limit int) ([]objectRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if limit <= 0 {
		limit = 100
	}
	records := make([]objectRecord, 0, limit)
	err := r.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		objectPrefix := objectBucketPrefix(bucket, prefix)
		for iter.Seek(objectPrefix); iter.ValidForPrefix(objectPrefix); iter.Next() {
			item := iter.Item()
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var record objectRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			records = append(records, record)
			if len(records) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	return records, nil
}

func objectKey(bucket, key string) []byte {
	return []byte("object/" + bucket + "\x00" + key)
}

func objectBucketPrefix(bucket, prefix string) []byte {
	return []byte("object/" + bucket + "\x00" + prefix)
}

func normalizeObjectPath(raw string) string {
	return strings.TrimPrefix(raw, "/")
}

func objectURL(bucket, key string) string {
	return "/b/" + bucket + "/" + key
}
