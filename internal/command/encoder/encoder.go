package encoder

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
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
		data, ok, err := channel.Consume("encoder.request")

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		var req queue.EncoderRequest

		err = yaml.Unmarshal(data, &req)

		if err != nil {
			log.Fatal(err)
		}

		encodedFile, err := encode(req.UID, bucket, req.Chunk, req.Params)

		if err != nil {
			log.Fatal(err)
		}

		yml, err := yaml.Marshal(queue.EncoderResponse{
			UID:       req.UID,
			ID:        encodedFile.ID,
			Qualities: encodedFile.Qualities,
		})

		if err != nil {
			log.Fatal()
		}

		if err = channel.Publish("encoder.response", yml); err != nil {
			log.Fatal(err)
		}
	}
}

type result struct {
	ID        int
	Qualities map[int]string
}

func encode(uid string, bucket storage.Bucket, chunkFilePath string, p []queue.Params) (*result, error) {
	chunkFile, err := bucket.Get(chunkFilePath)

	if err != nil {
		return nil, errors.Wrap(err, "unable to get master file from bucket")
	}

	workDir := "/tmp/" + uid
	_ = os.MkdirAll(workDir, os.ModePerm)

	var id int
	fileName := path.Base(chunkFilePath)

	if _, err = fmt.Sscanf(fileName, "%d.mp4", &id); err != nil {
		return nil, errors.Wrap(err, "unable to read chunk file id")
	}

	err = ioutil.WriteFile(workDir+"/"+fileName, chunkFile, os.ModePerm)

	if err != nil {
		return nil, errors.Wrap(err, "unable to write master file in workdir")
	}

	result := &result{ID: id}
	result.Qualities = make(map[int]string)

	list := make(map[int]string)

	exec := executor.NewExecutor(&bytes.Buffer{})
	cmd := &executor.Cmd{Binary: "ffmpeg"}

	cmd.Add("-i", workDir+"/"+fileName)
	cmd.Add("-hide_banner")
	cmd.Add("-y")

	for _, params := range p {
		cmd.Add("-dn") // no data
		cmd.Add("-dn") // no data
		cmd.Add("-map_metadata", "-1")
		cmd.Add("-map_chapters", "-1")
		cmd.Add("-map", fmt.Sprintf("0:%d", params.Map))

		cmd.Add("-c:v", params.Codec)

		if params.Profile != "" {
			cmd.Add("-profile:v", params.Profile)
		} else {
			cmd.Add("-profile:v", "high") // root, high, high422, high444
		}

		cmd.Add("-level", "4.2")
		//cmd.Add("-vf", fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", params.Width, params.Height))
		cmd.Add("-vf", "scale=-2:"+strconv.Itoa(params.Height)) // OR scale=1280:trunc(ow/a/2)*2
		cmd.Add("-b:v", params.BitRate)
		cmd.Add("-maxrate", params.MaxRate)
		cmd.Add("-minrate", params.MinRate)
		cmd.Add("-bufsize", params.BufSize)
		cmd.Add("-preset", params.Preset)
		cmd.Add("-crf", strconv.Itoa(params.CRF))

		cmd.Add("-sc_threshold", "0") // Sensitivity of x264's scenecut detection, 0 = no scene cut detection
		cmd.Add("-g", "60")           // GOP -> Group of Picture
		cmd.Add("-keyint_min", "48")  // Minimum GOP
		cmd.Add("-bf", "3")           // B-frames
		cmd.Add("-b_strategy", "2")

		if len(params.ExtraArgs) > 0 {
			cmd.Add(params.ExtraArgs...)
		}

		cmd.Add("-f", "mp4")

		_ = os.MkdirAll(fmt.Sprintf("%s/%d", workDir, params.Height), os.ModePerm)
		encodedFilePath := fmt.Sprintf("%s/%d/%03d.mp4", workDir, params.Height, id)
		cmd.Add(encodedFilePath)

		list[params.Height] = encodedFilePath
	}

	err = exec.Run(cmd)

	if err != nil {
		return nil, errors.Wrap(err, "unable to encode chunk with ffmpeg")
	}

	for quality, encodedFilePath := range list {
		data, err := ioutil.ReadFile(encodedFilePath)

		if err != nil {
			return nil, errors.Wrap(err, "unable to read encoded chunk file")
		}

		filePath := fmt.Sprintf("%s/encoded/%d/%03d.mp4", uid, quality, id)

		if err = bucket.Store(filePath, data); err != nil {
			return nil, errors.Wrap(err, "unable to store encoded chunk file")
		}

		result.Qualities[quality] = filePath
	}

	return result, nil
}
