package storage

import (
	"context"
	"io"

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

func (l *local) Store(key string, data []byte, _ ACL) error {
	return l.bucket.WriteAll(l.ctx, key, data, &blob.WriterOptions{})
}

func (l *local) Delete(key string) error {
	iter := l.bucket.List(&blob.ListOptions{
		Prefix: key,
	})

	for {
		obj, err := iter.Next(l.ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if obj.IsDir {
			continue
		}

		if err = l.bucket.Delete(l.ctx, obj.Key); err != nil {
			return err
		}
	}

	return nil
}
