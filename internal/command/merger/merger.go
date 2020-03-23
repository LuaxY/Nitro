package merger

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"trancode/internal/command/root"
	"trancode/internal/executor"
	"trancode/internal/queue"
	"trancode/internal/storage"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use: "merger",
	Run: func(cmd *cobra.Command, args []string) {
		Run()
	},
}

func Run() {
	bucket, err := storage.NewLocal(context.Background(), root.Cmd.Flag("storage").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	channel, err := queue.NewRabbitMQ(root.Cmd.Flag("amqp").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	for {
		data, ok, err := channel.Consume("merger.request")

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		var req queue.MergerRequest

		err = yaml.Unmarshal(data, &req)

		if err != nil {
			log.Fatal(err)
		}

		list := make(map[int]map[int]string)

		for id, qualities := range req.Chunks {
			for quality, chunk := range qualities {
				if list[quality] == nil {
					list[quality] = make(map[int]string)
				}
				list[quality][id] = chunk
			}
		}

		for quality, chunks := range list {
			final, err := merge(req.UID, bucket, quality, chunks)

			if err != nil {
				log.Fatal(err)
			}

			log.Println("FINAL", final.Path)
		}
	}
}

type result struct {
	Path string
}

func merge(uid string, bucket storage.Bucket, quality int, chunks map[int]string) (*result, error) {
	workDir, err := ioutil.TempDir(os.TempDir(), "merge")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary working directory")
	}

	f, err := os.Create(workDir + "/list.txt")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create chunk list file")
	}

	defer f.Close()

	// TODO sort chunks by key (id)

	var keys []int

	for k := range chunks {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	for _, k := range keys {
		chunk := chunks[k]
		chunkFile, err := bucket.Get(chunk)

		if err != nil {
			return nil, errors.Wrapf(err, "unable to get chunk file %s from bucket", chunk)
		}

		chunkFilePath := workDir + "/" + path.Base(chunk)

		if err = ioutil.WriteFile(chunkFilePath, chunkFile, os.ModePerm); err != nil {
			return nil, errors.Wrap(err, "unable to write chunk file")
		}

		if _, err = f.WriteString(fmt.Sprintf("file '%s'\n", chunkFilePath)); err != nil {
			return nil, errors.Wrap(err, "unable to add chunk to list file")
		}
	}

	if err = f.Close(); err != nil {
		return nil, errors.Wrap(err, "unable to close list file")
	}

	result := &result{}
	exec := executor.NewExecutor(&bytes.Buffer{})

	// Video

	cmd := &executor.Cmd{Binary: "ffmpeg"}
	cmd.Add("-f", "concat")
	cmd.Add("-safe", "0")
	cmd.Add("-i", workDir+"/list.txt")
	cmd.Add("-c", "copy")
	cmd.Add(workDir + "/final.mp4")
	err = exec.Run(cmd)

	if err != nil {
		return nil, errors.Wrap(err, "unable to merge chunk files with ffmpeg")
	}

	data, err := ioutil.ReadFile(workDir + "/final.mp4")

	if err != nil {
		return nil, errors.Wrap(err, "unable to read merged file")
	}

	finalPath := fmt.Sprintf("%s/%d.mp4", uid, quality)

	if err = bucket.Store(finalPath, data); err != nil {
		return nil, errors.Wrap(err, "unable to store merged file")
	}

	result.Path = finalPath

	return result, nil
}
