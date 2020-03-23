package database

type Database interface {
	Get(key string) (data string, err error)
	Set(key string, data string) (err error)
}
