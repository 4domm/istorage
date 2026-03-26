package store

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

var ErrNotFound = errors.New("chunk not found")

type Store struct {
	db *pebble.DB
}

func Open(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) PutIfAbsent(chunkID string, data []byte) (bool, error) {
	_, closer, err := s.db.Get([]byte(chunkID))
	if err == nil {
		if closer != nil {
			_ = closer.Close()
		}
		return true, nil
	}
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return false, err
	}
	if err := s.db.Set([]byte(chunkID), data, pebble.Sync); err != nil {
		return false, err
	}
	return false, nil
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
	return s.db.Delete([]byte(chunkID), pebble.Sync)
}

func (s *Store) BatchDelete(chunkIDs []string) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, chunkID := range chunkIDs {
		if err := batch.Delete([]byte(chunkID), nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}
