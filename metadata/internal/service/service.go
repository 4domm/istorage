package service

import (
	"context"
	"time"

	"sstorage/metadata/internal/model"
	fdbstore "sstorage/metadata/internal/store/fdb"
)

type Store interface {
	PutObject(ctx context.Context, bucket, key string, body model.PutObjectRequest) error
	GetObject(ctx context.Context, bucket, key string) (*model.ObjectMeta, error)
	DeleteObject(ctx context.Context, bucket, key string) (bool, error)
	ListBucketKeys(ctx context.Context, bucket string, limit int, cursor string) ([]string, *string, error)
	ChunkInUse(ctx context.Context, nodeID, chunkID string) (bool, error)
	GCNextBatch(ctx context.Context, limit int, owner string, lease time.Duration) ([]model.GcTask, error)
	GCAckBatch(ctx context.Context, owner string, seqs []uint64) (uint64, error)
}

type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) PutObject(ctx context.Context, bucket, key string, body model.PutObjectRequest) error {
	return s.store.PutObject(ctx, bucket, key, body)
}

func (s *Service) GetObject(ctx context.Context, bucket, key string) (*model.ObjectMeta, error) {
	return s.store.GetObject(ctx, bucket, key)
}

func (s *Service) DeleteObject(ctx context.Context, bucket, key string) (bool, error) {
	return s.store.DeleteObject(ctx, bucket, key)
}

func (s *Service) ListBucketKeys(ctx context.Context, bucket string, limit int, cursor string) ([]string, *string, error) {
	return s.store.ListBucketKeys(ctx, bucket, limit, cursor)
}

func (s *Service) ChunkInUse(ctx context.Context, nodeID, chunkID string) (bool, error) {
	return s.store.ChunkInUse(ctx, nodeID, chunkID)
}

func (s *Service) GCNextBatch(ctx context.Context, limit int, owner string, lease time.Duration) ([]model.GcTask, error) {
	return s.store.GCNextBatch(ctx, limit, owner, lease)
}

func (s *Service) GCAckBatch(ctx context.Context, owner string, seqs []uint64) (uint64, error) {
	return s.store.GCAckBatch(ctx, owner, seqs)
}

func IsNotFound(err error) bool {
	return err == fdbstore.ErrNotFound
}
