package encoder

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path"
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
	Use: "encoder",
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
		var req queue.EncoderRequest
		ok, err := channel.Consume("encoder.request", &req)

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		encodedFile, err := encode(req.UID, bucket, req.Chunk, req.Params)

		if err != nil {
			log.Fatal(err)
		}

		if err = channel.Publish("encoder.response", queue.EncoderResponse{
			UID:       req.UID,
			ID:        encodedFile.ID,
			Qualities: encodedFile.Qualities,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

type result struct {
	ID        int
	Qualities map[int]string
}

func encode(uid string, bucket storage.Bucket, chunkFilePath string, p []queue.Params) (*result, error) {
	workDir := "/tmp/" + uid
	_ = os.MkdirAll(workDir, os.ModePerm)

	var id int
	fileName := path.Base(chunkFilePath)

	if _, err := fmt.Sscanf(fileName, "%d.mp4", &id); err != nil {
		return nil, errors.Wrap(err, "unable to read chunk file id")
	}

	if err := util.Download(bucket, chunkFilePath, workDir+"/"+fileName); err != nil {
		return nil, errors.Wrap(err, "unable to get master file")
	}

	res := &result{ID: id}
	res.Qualities = make(map[int]string)

	list := make(map[int]string)

	exec := executor.NewExecutor(&bytes.Buffer{})
	ffmpeg := &executor.Cmd{Binary: "ffmpeg"}

	ffmpeg.Add("-i", workDir+"/"+fileName)
	ffmpeg.Add("-hide_banner")
	ffmpeg.Add("-y")

	for _, params := range p {
		ffmpeg.Add("-dn") // no data
		ffmpeg.Add("-map_metadata", "-1")
		ffmpeg.Add("-map_chapters", "-1")
		ffmpeg.Add("-map", fmt.Sprintf("0:%d", params.Map))

		ffmpeg.Add("-c:v", params.Codec)

		if params.Profile != "" {
			ffmpeg.Add("-profile:v", params.Profile)
		} else {
			ffmpeg.Add("-profile:v", "high") // root, high, high422, high444
		}

		ffmpeg.Add("-level", "4.2")
		//ffmpeg.Add("-vf", fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", params.Width, params.Height))
		ffmpeg.Add("-vf", "scale=-2:"+strconv.Itoa(params.Height)) // OR scale=1280:trunc(ow/a/2)*2
		ffmpeg.Add("-b:v", params.BitRate)
		ffmpeg.Add("-maxrate", params.MaxRate)
		ffmpeg.Add("-minrate", params.MinRate)
		ffmpeg.Add("-bufsize", params.BufSize)
		ffmpeg.Add("-preset", params.Preset)
		ffmpeg.Add("-crf", strconv.Itoa(params.CRF))

		ffmpeg.Add("-sc_threshold", "0") // Sensitivity of x264's scenecut detection, 0 = no scene cut detection
		ffmpeg.Add("-g", "60")           // GOP -> Group of Picture
		ffmpeg.Add("-keyint_min", "48")  // Minimum GOP
		ffmpeg.Add("-bf", "3")           // B-frames
		ffmpeg.Add("-b_strategy", "2")

		if len(params.ExtraArgs) > 0 {
			ffmpeg.Add(params.ExtraArgs...)
		}

		ffmpeg.Add("-f", "mp4")

		_ = os.MkdirAll(fmt.Sprintf("%s/%d", workDir, params.Height), os.ModePerm)
		encodedFilePath := fmt.Sprintf("%s/%d/%03d.mp4", workDir, params.Height, id)
		ffmpeg.Add(encodedFilePath)

		list[params.Height] = encodedFilePath
	}

	err := exec.Run(ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to encode chunk with ffmpeg")
	}

	for quality, encodedFilePath := range list {
		filePath := fmt.Sprintf("%s/encoded/%d/%03d.mp4", uid, quality, id)

		if err = util.Upload(bucket, filePath, encodedFilePath); err != nil {
			return nil, errors.Wrap(err, "unable to store encoded chunk file")
		}

		res.Qualities[quality] = filePath
	}

	return res, nil
}
