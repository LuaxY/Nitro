package splitter

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
		data, ok, err := channel.Consume("splitter.request")

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		var req queue.SplitterRequest

		err = yaml.Unmarshal(data, &req)

		if err != nil {
			log.Fatal(err)
		}

		splitedFiles, err := split(req.UID, bucket, req.Input, req.ChunkTime)

		if err != nil {
			log.Fatal(err)
		}

		// TODO send len(splitedFiles.Chunks) to watcher + audio

		for _, chunk := range splitedFiles.Chunks {
			yml, err := yaml.Marshal(queue.EncoderRequest{
				UID:    req.UID,
				Chunk:  chunk,
				Params: req.Params,
			})

			if err != nil {
				log.Fatal(err)
			}

			if err = channel.Publish("encoder.request", yml); err != nil {
				log.Fatal(err)
			}
		}
	}

	/*chunksList := make(map[string][]string)

	for _, chunk := range splitedFiles.Chunks {
		encodedFile, err := trancoder.Encode(uid, bucket, chunk, params)

		if err != nil {
			log.Fatal(err)
		}

		for quality, chunk := range encodedFile.Chunks {
			chunksList[quality] = append(chunksList[quality], chunk)
		}
	}*/

	/*for quality, chunks := range chunksList {
		final, err := merger.Merge(uid, bucket, quality, chunks)

		if err != nil {
			log.Fatal(err)
		}

		log.Println("FINAL", final.Path)
	}*/
}

type result struct {
	Chunks []string
	Audio  string
}

func split(uid string, bucket storage.Bucket, masterFilePath string, chunkTime int) (*result, error) {
	masterFile, err := bucket.Get(masterFilePath)

	if err != nil {
		return nil, errors.Wrap(err, "unable to get master file from bucket")
	}

	workDir := "/tmp/" + uid
	_ = os.MkdirAll(workDir, os.ModePerm)

	err = ioutil.WriteFile(workDir+"/master.mp4", masterFile, os.ModePerm)

	if err != nil {
		return nil, errors.Wrap(err, "unable to write master file in workdir")
	}

	result := &result{}
	exec := executor.NewExecutor(&bytes.Buffer{})

	// Video

	_ = os.MkdirAll(workDir+"/chunks", os.ModePerm)
	cmd := &executor.Cmd{Binary: "ffmpeg"}
	cmd.Add("-i", workDir+"/master.mp4")
	cmd.Add("-hide_banner", "-y", "-dn", "-map_metadata", "-1", "-map_chapters", "-1")
	cmd.Add("-map", "0:0") // TODO map
	cmd.Add("-c:v", "copy")
	cmd.Add("-f", "segment")
	cmd.Add("-segment_time", strconv.Itoa(chunkTime))
	cmd.Add(workDir + "/chunks/%03d.mp4")
	err = exec.Run(cmd)

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

		data, err := ioutil.ReadFile(path)

		if err != nil {
			return err
		}

		result.Chunks = append(result.Chunks, uid+"/chunks/"+info.Name())

		return bucket.Store(uid+"/chunks/"+info.Name(), data)
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to store chunk file")
	}

	// Audio

	cmd = &executor.Cmd{Binary: "ffmpeg"}
	cmd.Add("-i", workDir+"/master.mp4")
	cmd.Add("-hide_banner", "-y", "-dn", "-map_metadata", "-1", "-map_chapters", "-1")
	cmd.Add("-map", "0:1") // TODO map
	cmd.Add("-c:a", "aac")
	cmd.Add("-ac", "2")
	cmd.Add("-f", "mp4")
	cmd.Add(workDir + "/audio.m4a")
	err = exec.Run(cmd)

	if err != nil {
		return nil, errors.Wrap(err, "unable to extract audio from master file with ffmpeg")
	}

	data, err := ioutil.ReadFile(workDir + "/audio.m4a")

	if err != nil {
		return nil, errors.Wrap(err, "unable to read audio file")
	}

	if err = bucket.Store(uid+"/audio.m4a", data); err != nil {
		return nil, errors.Wrap(err, "unable to store audio file")
	}

	result.Audio = uid + "/audio.m4a"

	return result, nil
}
