package merger

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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
		var err error
		log.Info("starting merger")

		amqp := viper.GetString("amqp")
		channel, err := queue.NewRabbitMQ(amqp)

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to queue '%s'", amqp)
		}

		log.Infof("connected to queue '%s'", amqp)

		//bucketName := viper.GetString("storage")
		//bucket, err := storage.NewLocal(context.Background(), bucketName)

		bucketName := viper.GetString("aws-bucket")
		bucket, err := storage.NewS3(context.Background(), bucketName, &aws.Config{
			Endpoint:    aws.String(viper.GetString("aws-endpoint")),
			Region:      aws.String(viper.GetString("aws-region")),
			Credentials: credentials.NewStaticCredentials(viper.GetString("aws-id"), viper.GetString("aws-secret"), ""),
		})

		if err != nil {
			log.Infof("connected to storage '%s'", bucketName)
		}

		log.Infof("connected to storage '%s'", bucketName)

		m := merger{
			channel: channel,
			bucket:  bucket,
		}

		m.Run()
	},
}

type merger struct {
	channel queue.Channel
	bucket  storage.Bucket
}

func (m *merger) Run() {
	log.Info("merger started")

	for {
		var req queue.MergerRequest
		ok, err := m.channel.Consume("merger.request", &req)

		if err != nil {
			log.WithError(err).Error("unable to consume merger.request")
			time.Sleep(5 * time.Second)
			continue
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		if err = m.HandleMerger(req); err != nil {
			log.WithError(err).Error("error while handling merger")
		}
	}
}

func (m *merger) HandleMerger(req queue.MergerRequest) error {
	log.WithFields(log.Fields{
		"app": "merger",
		"uid": req.UID,
	}).Info("receive merger request")

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
		_, err := merge(req.UID, m.bucket, quality, chunks)

		if err != nil {
			return errors.Wrapf(err, "error while merging '%s' %d", req.UID, quality)
		}

		qualities = append(qualities, quality)
	}

	if err := m.channel.Publish("merger.response", queue.MergerResponse{
		UID:       req.UID,
		Qualities: qualities,
	}); err != nil {
		return errors.Wrap(err, "unable to publish in merger.response")
	}

	log.WithFields(log.Fields{
		"app":       "merger",
		"uid":       req.UID,
		"qualities": qualities,
	}).Info("send merger response")

	return nil
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

	defer os.RemoveAll(workDir)

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
