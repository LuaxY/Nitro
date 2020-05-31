package storage

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

type s3 struct {
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

	return &s3{bucket: bucket}, nil
}

func (s *s3) Get(ctx context.Context, key string) (data []byte, err error) {
	return s.bucket.ReadAll(ctx, key)
}

func (s *s3) Read(ctx context.Context, key string, output io.Writer) (err error) {
	reader, err := s.bucket.NewReader(ctx, key, nil)

	if err != nil {
		return err
	}

	defer reader.Close()

	_, err = io.Copy(output, reader)
	return err
}

func (s *s3) Store(ctx context.Context, key string, data []byte, acl ACL) error {
	before := func(asFunc func(interface{}) bool) error {
		req := &s3manager.UploadInput{}
		ok := asFunc(&req)
		if !ok {
			return errors.New("invalid s3 type")
		}
		req.ACL = aws.String(string(acl))
		return nil
	}

	return s.bucket.WriteAll(ctx, key, data, &blob.WriterOptions{BeforeWrite: before})
}

func (s *s3) Write(ctx context.Context, key string, input io.Reader, acl ACL) error {
	before := func(asFunc func(interface{}) bool) error {
		req := &s3manager.UploadInput{}
		ok := asFunc(&req)
		if !ok {
			return errors.New("invalid s3 type")
		}
		req.ACL = aws.String(string(acl))
		return nil
	}

	writer, err := s.bucket.NewWriter(ctx, key, &blob.WriterOptions{BeforeWrite: before})

	if err != nil {
		return err
	}

	defer writer.Close()

	_, err = io.Copy(writer, input)
	return err
}

func (s *s3) Delete(ctx context.Context, key string) error {
	iter := s.bucket.List(&blob.ListOptions{
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

		if err = s.bucket.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}

	return nil
}
