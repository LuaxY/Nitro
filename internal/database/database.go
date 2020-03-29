package database

import "time"

type Database interface {
	Get(key string) (data string, err error)
	Set(key string, data string, expiration time.Duration) (err error)
	Delete(key string) (err error)
}
