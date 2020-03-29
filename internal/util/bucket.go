package util

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"

	"trancode/internal/storage"
)

func Download(bucket storage.Bucket, key string, path string) error {
	log.Debugf("download '%s' to '%s'", key, path)

	data, err := bucket.Get(key)

	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, data, os.ModePerm)
}

func Upload(bucket storage.Bucket, key string, path string, acl storage.ACL) error {
	log.Debugf("upload '%s' to '%s'", path, key)

	data, err := ioutil.ReadFile(path)

	if err != nil {
		return err
	}

	return bucket.Store(key, data, acl)
}
