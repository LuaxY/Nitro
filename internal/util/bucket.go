package util

import (
	"os"

	log "github.com/sirupsen/logrus"

	"trancode/internal/storage"
)

func Download(bucket storage.Bucket, key string, path string) error {
	log.Debugf("download '%s' to '%s'", key, path)

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, os.ModePerm)

	if err != nil {
		return err
	}

	defer file.Close()

	return bucket.Read(key, file)
}

func Upload(bucket storage.Bucket, key string, path string, acl storage.ACL) error {
	log.Debugf("upload '%s' to '%s'", path, key)

	file, err := os.Open(path)

	if err != nil {
		return err
	}

	defer file.Close()

	return bucket.Write(key, file, acl)
}
