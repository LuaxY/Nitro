package storage

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

type s3 struct {
	ctx    context.Context
	bucket *blob.Bucket
}

func NewS3(ctx context.Context, bucketName string, config *aws.Config) (Bucket, error) {
	sess, err := session.NewSession(config)

	if err != nil {
		return nil, err
	}

	bucket, err := s3blob.OpenBucket(ctx, sess, bucketName, nil)

	if err != nil {
		return nil, err
	}

	return &s3{ctx: ctx, bucket: bucket}, nil
}

func (s *s3) Get(key string) (data []byte, err error) {
	return s.bucket.ReadAll(s.ctx, key)
}

func (s *s3) Store(key string, data []byte) error {
	return s.bucket.WriteAll(s.ctx, key, data, &blob.WriterOptions{})
}

func (s *s3) Delete(key string) error {
	iter := s.bucket.List(&blob.ListOptions{
		Prefix: key,
	})

	for {
		obj, err := iter.Next(s.ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if obj.IsDir {
			continue
		}

		if err = s.bucket.Delete(s.ctx, obj.Key); err != nil {
			return err
		}
	}

	return nil
}
