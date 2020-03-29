package packager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
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
	Use: "packager",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		log.Info("starting packager")

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
			log.WithError(err).Fatalf("unable to connect to storage '%s'", bucketName)
		}

		log.Infof("connected to storage '%s'", bucketName)

		p := packager{
			channel: channel,
			bucket:  bucket,
		}

		p.Run()
	},
}

type packager struct {
	channel queue.Channel
	bucket  storage.Bucket
}

func (p *packager) Run() {
	log.Info("packager started")

	for {
		var req queue.PackagerRequest
		ok, err := p.channel.Consume("packager.request", &req)

		if err != nil {
			log.WithError(err).Error("unable to consume packager.request")
			time.Sleep(5 * time.Second)
			continue
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		if err = p.HandlePackager(req); err != nil {
			log.WithError(err).Error("error while handling packager")
		}
	}
}

func (p *packager) HandlePackager(req queue.PackagerRequest) error {
	log.WithFields(log.Fields{
		"app":       "packager",
		"uid":       req.UID,
		"qualities": req.Qualities,
	}).Info("receive merger request")

	_, err := pack(req.UID, p.bucket, req.Qualities)

	if err != nil {
		return errors.Wrapf(err, "error while packing '%s'", req.UID)
	}

	if err = p.channel.Publish("packager.response", queue.PackagerResponse{
		UID: req.UID,
	}); err != nil {
		return errors.Wrap(err, "unable to publish in packager.response")
	}

	log.WithFields(log.Fields{
		"app": "packager",
		"uid": req.UID,
	}).Info("send packager response")

	return nil
}

type result struct {
	Path string
}

func pack(uid string, bucket storage.Bucket, qualities []int) (*result, error) {
	workDir, err := ioutil.TempDir(os.TempDir(), "pack")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary working directory")
	}

	defer os.RemoveAll(workDir)

	if err = util.Download(bucket, uid+"/audio.m4a", workDir+"/audio.m4a"); err != nil {
		return nil, errors.Wrap(err, "unable to get audio file")
	}

	files := make(map[string]string)
	exec := executor.NewExecutor(&bytes.Buffer{})
	packager := &executor.Cmd{Binary: "packager"}

	for _, quality := range qualities {
		if err = util.Download(bucket, fmt.Sprintf("%s/%d.mp4", uid, quality), fmt.Sprintf("%s/%d.mp4", workDir, quality)); err != nil {
			return nil, errors.Wrap(err, "unable to get video file")
		}

		packager.Add(fmt.Sprintf("in=%[1]s/%[2]d.mp4,stream=video,output=%[1]s/out/%[2]d.mp4,playlist_name=%[1]s/out/%[2]d.m3u8,iframe_playlist_name=%[1]s/out/%[2]d_iframe.m3u8", workDir, quality))

		files[fmt.Sprintf("%s/%d.mp4", uid, quality)] = fmt.Sprintf("%s/out/%d.mp4", workDir, quality)
		files[fmt.Sprintf("%s/%d.m3u8", uid, quality)] = fmt.Sprintf("%s/out/%d.m3u8", workDir, quality)
		files[fmt.Sprintf("%s/%d_iframe.m3u8", uid, quality)] = fmt.Sprintf("%s/out/%d_iframe.m3u8", workDir, quality)
	}

	packager.Add(fmt.Sprintf("in=%[1]s/audio.m4a,stream=audio,output=%[1]s/out/audio.m4a,playlist_name=%[1]s/out/audio.m3u8,hls_group_id=audio", workDir))
	packager.Add("--hls_master_playlist_output", fmt.Sprintf("%s/out/master.m3u8", workDir))
	packager.Add("--mpd_output", fmt.Sprintf("%s/out/manifest.mpd", workDir))

	if err := exec.Run(packager); err != nil {
		return nil, errors.Wrap(err, "error while executing packager")
	}

	files[uid+"/audio.m4a"] = workDir + "/out/audio.m4a"
	files[uid+"/audio.m3u8"] = workDir + "/out/audio.m3u8"
	files[uid+"/master.m3u8"] = workDir + "/out/master.m3u8"
	files[uid+"/manifest.mpd"] = workDir + "/out/manifest.mpd"

	for key, file := range files {
		if err = util.Upload(bucket, key, file); err != nil {
			return nil, errors.Wrapf(err, "unable to upload %s file", file)
		}
	}

	return &result{}, nil
}
