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

	"trancode/internal/command/root"
	"trancode/internal/executor"
	"trancode/internal/queue"
	"trancode/internal/storage"
	"trancode/internal/util"
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
		var req queue.MergerRequest
		ok, err := channel.Consume("merger.request", &req)

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
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

		var qualities []int

		for quality, chunks := range list {
			_, err := merge(req.UID, bucket, quality, chunks)

			if err != nil {
				log.Fatal(err)
			}

			qualities = append(qualities, quality)
		}

		if err = channel.Publish("merger.response", queue.MergerResponse{
			UID:       req.UID,
			Qualities: qualities,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

// TODO result needed ?
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

	var keys []int

	for k := range chunks {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	for _, k := range keys {
		chunk := chunks[k]
		chunkFilePath := workDir + "/" + path.Base(chunk)

		if err = util.Download(bucket, chunk, chunkFilePath); err != nil {
			return nil, errors.Wrap(err, "unable to get chunk file")
		}

		if _, err = f.WriteString(fmt.Sprintf("file '%s'\n", chunkFilePath)); err != nil {
			return nil, errors.Wrap(err, "unable to add chunk to list file")
		}
	}

	if err = f.Close(); err != nil {
		return nil, errors.Wrap(err, "unable to close list file")
	}

	res := &result{}
	exec := executor.NewExecutor(&bytes.Buffer{})

	// Video

	ffmpeg := &executor.Cmd{Binary: "ffmpeg"}
	ffmpeg.Add("-f", "concat")
	ffmpeg.Add("-safe", "0")
	ffmpeg.Add("-i", workDir+"/list.txt")
	ffmpeg.Add("-c", "copy")
	ffmpeg.Add(workDir + "/merged.mp4")
	err = exec.Run(ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to merge chunk files with ffmpeg")
	}

	finalPath := fmt.Sprintf("%s/%d.mp4", uid, quality)

	if err = util.Upload(bucket, finalPath, workDir+"/merged.mp4"); err != nil {
		return nil, errors.Wrap(err, "unable to store merged file")
	}

	res.Path = finalPath

	return res, nil
}
