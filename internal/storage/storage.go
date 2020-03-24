package storage

import (
	_ "gocloud.dev/blob/fileblob"
)

type Bucket interface {
	Get(key string) (data []byte, err error)
	Store(key string, data []byte) (err error)
	Delete(key string) (err error)
}
