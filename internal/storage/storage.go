package storage

import (
	"context"
	"io"

	_ "gocloud.dev/blob/fileblob"
)

type Bucket interface {
	Get(ctx context.Context, key string) (data []byte, err error)
	Read(ctx context.Context, key string, output io.Writer) (err error)
	Store(ctx context.Context, key string, data []byte, acl ACL) (err error)
	Write(ctx context.Context, key string, input io.Reader, acl ACL) (err error)
	Delete(ctx context.Context, key string) (err error)
}

type ACL string

const (
	PublicACL  ACL = "public-read"
	PrivateACL ACL = "private"
)
