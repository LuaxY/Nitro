package storage

import (
	"context"
	"io"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
)

type GCS struct {
	bucket *blob.Bucket
}

func NewGCS(ctx context.Context, bucketName string, client *gcp.HTTPClient) (Bucket, error) {
	bucket, err := gcsblob.OpenBucket(ctx, client, bucketName, nil)

	if err != nil {
		return nil, err
	}

	return &GCS{bucket: bucket}, nil
}

func (g *GCS) Get(ctx context.Context, key string) (data []byte, err error) {
	return g.bucket.ReadAll(ctx, key)
}

func (g *GCS) Read(ctx context.Context, key string, output io.Writer) (err error) {
	reader, err := g.bucket.NewReader(ctx, key, nil)

	if err != nil {
		return err
	}

	defer reader.Close()

	_, err = io.Copy(output, reader)
	return err
}

func (g *GCS) Store(ctx context.Context, key string, data []byte, acl ACL) error {
	/*before := func(as func(interface{}) bool) error {
		var objp **storage.ObjectHandle
		ok := as(&objp)
		if !ok {
			return errors.New("invalid GCS type")
		}
		return (*objp).ACL().Set(ctx, storage.AllUsers, storage.RoleReader)
	}*/

	return g.bucket.WriteAll(ctx, key, data, nil /*&blob.WriterOptions{BeforeWrite: before}*/)
}

func (g *GCS) Write(ctx context.Context, key string, input io.Reader, acl ACL) error {
	/*before := func(as func(interface{}) bool) error {
		var objp **storage.ObjectHandle
		ok := as(&objp)
		if !ok {
			return errors.New("invalid GCS type")
		}
		return (*objp).ACL().Set(ctx, storage.AllUsers, storage.RoleReader)
	}*/

	writer, err := g.bucket.NewWriter(ctx, key, nil /*&blob.WriterOptions{BeforeWrite: before}*/)

	if err != nil {
		return err
	}

	defer writer.Close()

	_, err = io.Copy(writer, input)
	return err
}

func (g *GCS) Delete(ctx context.Context, key string) error {
	iter := g.bucket.List(&blob.ListOptions{
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

		if err = g.bucket.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}

	return nil
}
