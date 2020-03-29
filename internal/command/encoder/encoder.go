package encoder

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"trancode/internal/command/root"
	"trancode/internal/executor"
	"trancode/internal/metric"
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
		log.Info("starting encoder")

		cmpt := root.GetComponent(false, true, true, true)

		e := encoder{
			channel: cmpt.Channel,
			bucket:  cmpt.Bucket,
			metric:  cmpt.Metric,
		}

		e.Run()
	},
}

type encoder struct {
	channel queue.Channel
	bucket  storage.Bucket
	metric  metric.Client
}

func (e *encoder) Run() {
	log.Info("encoder started")

	go e.metric.Ticker(context.Background(), 1*time.Second)

	hostname, _ := os.Hostname()

	counterMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_encoder_tasks_total", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	gaugeMetric := &metric.GaugeMetric{
		RowMetric: metric.RowMetric{Name: "nitro_encoder_tasks_count", Tags: metric.Tags{"hostname": hostname}},
		Gauge:     0,
	}

	e.metric.Add(counterMetric)
	e.metric.Add(gaugeMetric)

	for {
		var req queue.EncoderRequest
		ok, err := e.channel.Consume("encoder.request", &req)

		if err != nil {
			log.WithError(err).Error("unable to consume encoder.request")
			time.Sleep(5 * time.Second)
			continue
		}

		if !ok {
			gaugeMetric.Gauge = 0
			time.Sleep(5 * time.Second)
			continue
		}

		counterMetric.Counter++
		gaugeMetric.Gauge = 1

		started := time.Now()

		if err = e.HandleEncoder(req); err != nil {
			log.WithError(err).Error("error while handling encoder")
		}

		durationMetric := &metric.DurationMetric{
			RowMetric: metric.RowMetric{Name: "nitro_encoder_tasks_duration", Tags: metric.Tags{"hostname": hostname, "uid": req.UID}},
			Duration:  time.Since(started),
		}
		e.metric.Send(durationMetric.Metric())
	}
}

type result struct {
	ID        int
	Qualities map[int]string
}

func (e *encoder) HandleEncoder(req queue.EncoderRequest) error {
	log.WithFields(log.Fields{
		"app":   "encoder",
		"uid":   req.UID,
		"chunk": req.Chunk,
	}).Info("receive encoder request")

	encodedFile, err := encode(req.UID, e.bucket, req.Chunk, req.Params)

	if err != nil {
		return errors.Wrapf(err, "error while encoding '%s'", req.UID)
	}

	if err = e.channel.Publish("encoder.response", queue.EncoderResponse{
		UID:       req.UID,
		ID:        encodedFile.ID,
		Qualities: encodedFile.Qualities,
	}); err != nil {
		return errors.Wrap(err, "unable to publish in encoder.response")
	}

	log.WithFields(log.Fields{
		"app":       "encoder",
		"uid":       req.UID,
		"qualities": encodedFile.Qualities,
	}).Info("send encoder response")

	return nil
}

func encode(uid string, bucket storage.Bucket, chunkFilePath string, p []queue.Params) (*result, error) {
	workDir, err := ioutil.TempDir(os.TempDir(), "encode")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary working directory")
	}

	defer os.RemoveAll(workDir)

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

	err = exec.Run(ffmpeg)

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