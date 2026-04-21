package store

import (
	"errors"
	"sort"

	"github.com/cockroachdb/pebble"
)

var ErrNotFound = errors.New("chunk not found")

type Store struct {
	db       *pebble.DB
	writeOpt *pebble.WriteOptions
}

type Tuning struct {
	MemTableSizeMB           int
	L0CompactionThreshold    int
	L0StopWritesThreshold    int
	MaxConcurrentCompactions int
}

type ChunkWrite struct {
	ChunkID string
	Data    []byte
}

func Open(path string, syncWrites bool, tuning Tuning) (*Store, error) {
	options := &pebble.Options{}
	if tuning.MemTableSizeMB > 0 {
		options.MemTableSize = uint64(tuning.MemTableSizeMB) * 1024 * 1024
	}
	if tuning.L0CompactionThreshold > 0 {
		options.L0CompactionThreshold = tuning.L0CompactionThreshold
	}
	if tuning.L0StopWritesThreshold > 0 {
		options.L0StopWritesThreshold = tuning.L0StopWritesThreshold
	}
	if tuning.MaxConcurrentCompactions > 0 {
		maxCompactions := tuning.MaxConcurrentCompactions
		options.MaxConcurrentCompactions = func() int {
			return maxCompactions
		}
	}

	db, err := pebble.Open(path, options)
	if err != nil {
		return nil, err
	}
	writeOpt := pebble.NoSync
	if syncWrites {
		writeOpt = pebble.Sync
	}
	return &Store{db: db, writeOpt: writeOpt}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Put(chunkID string, data []byte) error {
	return s.db.Set([]byte(chunkID), data, s.writeOpt)
}

func (s *Store) Get(chunkID string) ([]byte, error) {
	value, closer, err := s.db.Get([]byte(chunkID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), nil
}

func (s *Store) Delete(chunkID string) error {
	return s.db.Delete([]byte(chunkID), s.writeOpt)
}

func (s *Store) Exists(chunkID string) (bool, error) {
	_, closer, err := s.db.Get([]byte(chunkID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return true, nil
}

func (s *Store) BatchMissing(chunkIDs []string) ([]string, error) {
	missingSet := make(map[string]struct{}, len(chunkIDs))
	unique := make([]string, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		if _, ok := missingSet[chunkID]; ok {
			continue
		}
		missingSet[chunkID] = struct{}{}
		unique = append(unique, chunkID)
	}
	sort.Strings(unique)

	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for _, chunkID := range unique {
		ok := iter.SeekGE([]byte(chunkID))
		if !ok || string(iter.Key()) != chunkID {
			missingSet[chunkID] = struct{}{}
		} else {
			delete(missingSet, chunkID)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	missing := make([]string, 0, len(missingSet))
	for chunkID := range missingSet {
		missing = append(missing, chunkID)
	}
	sort.Strings(missing)
	return missing, nil
}

func (s *Store) BatchDelete(chunkIDs []string) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, chunkID := range chunkIDs {
		if err := batch.Delete([]byte(chunkID), nil); err != nil {
			return err
		}
	}
	return batch.Commit(s.writeOpt)
}

func (s *Store) BatchPut(items []ChunkWrite) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, item := range items {
		if err := batch.Set([]byte(item.ChunkID), item.Data, nil); err != nil {
			return err
		}
	}
	return batch.Commit(s.writeOpt)
}
