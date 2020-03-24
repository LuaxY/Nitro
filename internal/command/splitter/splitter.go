package splitter

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
	Use: "splitter",
	Run: func(cmd *cobra.Command, args []string) {
		Run()
	},
}

func Run() {
	// TODO S3
	bucket, err := storage.NewLocal(context.Background(), root.Cmd.Flag("storage").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	channel, err := queue.NewRabbitMQ(root.Cmd.Flag("amqp").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	for {
		var req queue.SplitterRequest
		ok, err := channel.Consume("splitter.request", &req)

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		splittedFiles, err := split(req.UID, bucket, req.Input, req.ChunkTime)

		if err != nil {
			log.Fatal(err)
		}

		if err = channel.Publish("splitter.response", queue.SplitterResponse{
			UID:         req.UID,
			TotalChunks: len(splittedFiles.Chunks),
			Chunks:      splittedFiles.Chunks,
			Params:      req.Params,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

type result struct {
	Chunks []string
	Audio  string
}

func split(uid string, bucket storage.Bucket, masterFilePath string, chunkTime int) (*result, error) {
	workDir := "/tmp/" + uid
	_ = os.MkdirAll(workDir, os.ModePerm)

	if err := util.Download(bucket, masterFilePath, workDir+"/master.mp4"); err != nil {
		return nil, errors.Wrap(err, "unable get master file")
	}

	res := &result{}
	exec := executor.NewExecutor(&bytes.Buffer{})

	// Video

	_ = os.MkdirAll(workDir+"/chunks", os.ModePerm)
	ffmpeg := &executor.Cmd{Binary: "ffmpeg"}
	ffmpeg.Add("-i", workDir+"/master.mp4")
	ffmpeg.Add("-hide_banner", "-y", "-dn", "-map_metadata", "-1", "-map_chapters", "-1")
	ffmpeg.Add("-map", "0:0") // TODO map
	ffmpeg.Add("-c:v", "copy")
	ffmpeg.Add("-f", "segment")
	ffmpeg.Add("-segment_time", strconv.Itoa(chunkTime))
	ffmpeg.Add(workDir + "/chunks/%03d.mp4")
	err := exec.Run(ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to split master file with ffmpeg")
	}

	err = filepath.Walk(workDir+"/chunks", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		res.Chunks = append(res.Chunks, uid+"/chunks/"+info.Name())
		return util.Upload(bucket, uid+"/chunks/"+info.Name(), path)
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to store chunk file")
	}

	// Audio

	ffmpeg = &executor.Cmd{Binary: "ffmpeg"}
	ffmpeg.Add("-i", workDir+"/master.mp4")
	ffmpeg.Add("-hide_banner", "-y", "-dn", "-map_metadata", "-1", "-map_chapters", "-1")
	ffmpeg.Add("-map", "0:1") // TODO map
	ffmpeg.Add("-c:a", "aac")
	ffmpeg.Add("-ac", "2")
	ffmpeg.Add("-f", "mp4")
	ffmpeg.Add(workDir + "/audio.m4a")
	err = exec.Run(ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to extract audio from master file with ffmpeg")
	}

	if err = util.Upload(bucket, uid+"/audio.m4a", workDir+"/audio.m4a"); err != nil {
		return nil, errors.Wrap(err, "unable to store audio file")
	}

	res.Audio = uid + "/audio.m4a"

	return res, nil
}
