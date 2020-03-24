package util

import (
	"io/ioutil"
	"log"
	"os"

	"trancode/internal/storage"
)

func Download(bucket storage.Bucket, key string, path string) error {
	log.Printf("download '%s' to '%s'", key, path)

	data, err := bucket.Get(key)

	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, data, os.ModePerm)
}

func Upload(bucket storage.Bucket, key string, path string) error {
	log.Printf("upload '%s' to '%s'", path, key)

	data, err := ioutil.ReadFile(path)

	if err != nil {
		return err
	}

	return bucket.Store(key, data)
}
