package database

import (
	"time"

	"github.com/go-redis/redis/v7"
)

type redisDb struct {
	client *redis.Client
}

func NewRedis(options *redis.Options) (Database, error) {
	client := redis.NewClient(options)

	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}

	return &redisDb{client: client}, nil
}

func (r *redisDb) Get(key string) (data string, err error) {
	return r.client.Get(key).Result()
}

func (r *redisDb) Set(key string, data string, expiration time.Duration) (err error) {
	return r.client.Set(key, data, expiration).Err()
}

func (r *redisDb) Delete(key string) (err error) {
	return r.client.Del(key).Err()
}
