package splitter

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
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
	Use: "splitter",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		log.Info("starting splitter")

		amqp := viper.GetString("amqp")
		channel, err := queue.NewRabbitMQ(context.Background(), amqp)

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

		s := splitter{
			channel: channel,
			bucket:  bucket,
		}

		s.Run()
	},
}

type splitter struct {
	channel queue.Channel
	bucket  storage.Bucket
}

func (s *splitter) Run() {
	log.Info("splitter started")

	for {
		var req queue.SplitterRequest
		ok, err := s.channel.Consume("splitter.request", &req)

		if err != nil {
			log.WithError(err).Error("unable to consume splitter.request")
			time.Sleep(5 * time.Second)
			continue
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		if err = s.HandleSplitter(req); err != nil {
			log.WithError(err).Error("error while handling splitter")
		}
	}
}

func (s *splitter) HandleSplitter(req queue.SplitterRequest) error {
	log.WithFields(log.Fields{
		"app":        "splitter",
		"uid":        req.UID,
		"input":      req.Input,
		"chunk_time": req.ChunkTime,
	}).Info("receive splitter request")

	splittedFiles, err := split(req.UID, s.bucket, req.Input, req.ChunkTime)

	if err != nil {
		return errors.Wrapf(err, "error while splitting '%s'", req.UID)
	}

	if err = s.channel.Publish("splitter.response", queue.SplitterResponse{
		UID:         req.UID,
		TotalChunks: len(splittedFiles.Chunks),
		Chunks:      splittedFiles.Chunks,
		Params:      req.Params,
	}); err != nil {
		return errors.Wrap(err, "unable to publish in splitter.response")
	}

	log.WithFields(log.Fields{
		"app":          "splitter",
		"uid":          req.UID,
		"total_chunks": len(splittedFiles.Chunks),
	}).Info("send splitter response")

	return nil
}

type result struct {
	Chunks []string
	Audio  string
}

func split(uid string, bucket storage.Bucket, masterFilePath string, chunkTime int) (*result, error) {
	workDir, err := ioutil.TempDir(os.TempDir(), "split")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary working directory")
	}

	defer os.RemoveAll(workDir)

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
	err = exec.Run(ffmpeg)

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
