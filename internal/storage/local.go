package storage

import (
	"context"

	"gocloud.dev/blob"
)

type local struct {
	ctx    context.Context
	bucket *blob.Bucket
}

func NewLocal(ctx context.Context, path string) (Bucket, error) {
	bucket, err := blob.OpenBucket(ctx, "file://"+path)

	if err != nil {
		return nil, err
	}

	return &local{ctx: ctx, bucket: bucket}, nil
}

func (l *local) Get(key string) (data []byte, err error) {
	return l.bucket.ReadAll(l.ctx, key)
}

func (l *local) Store(key string, data []byte) error {
	return l.bucket.WriteAll(l.ctx, key, data, &blob.WriterOptions{})
}
