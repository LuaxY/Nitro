package util

import (
	"context"
	"os"
	"time"

	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"

	"nitro/internal/storage"
)

func Download(ctx context.Context, bucket storage.Bucket, key string, path string) error {
	return retry.Do(func() error {
		log.Debugf("download '%s' to '%s'", key, path)

		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, os.ModePerm)

		if err != nil {
			return err
		}

		defer file.Close()

		return bucket.Read(ctx, key, file)
	}, retry.Attempts(3), retry.LastErrorOnly(true), retry.OnRetry(func(n uint, err error) {
		log.WithError(err).Errorf("download retry %d", n)
	}))
}

func Upload(ctx context.Context, bucket storage.Bucket, key string, path string, acl storage.ACL) error {
	return retry.Do(func() error {
		log.Debugf("upload '%s' to '%s'", path, key)

		file, err := os.Open(path)

		if err != nil {
			return err
		}

		defer file.Close()

		ctx, _ = context.WithTimeout(ctx, 2*time.Minute) // TODO move timeout away ?

		return bucket.Write(ctx, key, file, acl)
	}, retry.Attempts(3), retry.LastErrorOnly(true), retry.OnRetry(func(n uint, err error) {
		log.WithError(err).Errorf("upload retry %d", n)
	}))
}
