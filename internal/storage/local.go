package storage

import (
	"context"
	"io"

	"gocloud.dev/blob"
)

type local struct {
	bucket *blob.Bucket
}

func NewLocal(ctx context.Context, path string) (Bucket, error) {
	bucket, err := blob.OpenBucket(ctx, "file://"+path)

	if err != nil {
		return nil, err
	}

	return &local{bucket: bucket}, nil
}

func (l *local) Get(ctx context.Context, key string) (data []byte, err error) {
	return l.bucket.ReadAll(ctx, key)
}

func (l *local) Read(ctx context.Context, key string, output io.Writer) (err error) {
	reader, err := l.bucket.NewReader(ctx, key, nil)

	if err != nil {
		return err
	}

	defer reader.Close()

	_, err = io.Copy(output, reader)
	return err
}

func (l *local) Store(ctx context.Context, key string, data []byte, _ ACL) error {
	return l.bucket.WriteAll(ctx, key, data, &blob.WriterOptions{})
}

func (l *local) Write(ctx context.Context, key string, input io.Reader, _ ACL) error {
	writer, err := l.bucket.NewWriter(ctx, key, nil)

	if err != nil {
		return err
	}

	defer writer.Close()

	_, err = io.Copy(writer, input)
	return err
}

func (l *local) Delete(ctx context.Context, key string) error {
	iter := l.bucket.List(&blob.ListOptions{
		Prefix: key,
	})

	for {
		obj, err := iter.Next(ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if obj.IsDir {
			continue
		}

		if err = l.bucket.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}

	return nil
}
