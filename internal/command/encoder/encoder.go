package encoder

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"nitro/internal/command/root"
	"nitro/internal/executor"
	"nitro/internal/metric"
	"nitro/internal/queue"
	"nitro/internal/signal"
	"nitro/internal/storage"
	"nitro/internal/transcode"
	"nitro/internal/util"
)

var (
	logger = log.WithFields(log.Fields{
		"app":     "encoder",
		"version": "dev",
	})
)

func init() {
	root.Cmd.AddCommand(cmd)

	cmd.PersistentFlags().String("provider", "unknown", "Cloud provider")
	cmd.PersistentFlags().Bool("cuda", false, "Enable CUDA")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		logger.WithError(err).Fatal("flag biding failed")
	}
}

var cmd = &cobra.Command{
	Use:   "encoder",
	Short: "Encode video chunks",
	Long:  `Nitro Encoder: encode requested video chunk in different qualities`,
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("starting encoder")

		cmpt := root.GetComponent(false, true, true, true)

		e := encoder{
			channel:  cmpt.Channel,
			bucket:   cmpt.Bucket,
			metric:   cmpt.Metric,
			provider: viper.GetString("provider"),
		}

		e.Run()
	},
}

type encoder struct {
	channel  queue.Channel
	bucket   storage.Bucket
	metric   metric.Client
	provider string
}

func (e *encoder) Run() {
	ctx := signal.WatchInterrupt(context.Background(), 25*time.Second)

	logger.Info("encoder started")

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

	errorsMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_encoder_tasks_errors", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	e.metric.Add(counterMetric)
	e.metric.Add(gaugeMetric)
	e.metric.Add(errorsMetric)

	var last struct {
		request  *queue.EncoderRequest
		delivery queue.Delivery
	}

	encoderStarted := time.Now()
	noMessageCounter := 0

loop:
	for {
		select {
		case <-ctx.Done():
			// Requeue last chunk if shutdown signal is receive
			if last.request != nil {
				if err := last.delivery.Nack(true); err != nil {
					logger.WithError(err).Error("unable to requeue last chunk")
					break loop
				}

				logger.WithFields(log.Fields{
					"uid":   last.request.UID,
					"chunk": last.request.Chunk,
				}).Info("requeue last chunk (#1)")
			}

			break loop
		default:
			if noMessageCounter >= 3 {
				logger.Infof("no messages after %d retry, shutdown", noMessageCounter)
				break loop
			}

			var req queue.EncoderRequest
			ok, msg, err := e.channel.Consume("encoder.request", &req)

			if err != nil {
				logger.WithError(err).Error("unable to consume encoder.request")
				time.Sleep(5 * time.Second)
				continue
			}

			if !ok {
				noMessageCounter++
				gaugeMetric.Gauge = 0
				time.Sleep(5 * time.Second)
				continue
			}

			noMessageCounter = 0

			last.request = &req
			last.delivery = msg

			counterMetric.Counter++
			gaugeMetric.Gauge = 1

			taskStarted := time.Now()

			if err = e.HandleEncoder(ctx, req); err != nil {
				if strings.Contains(err.Error(), "signal: killed") {
					if err := last.delivery.Nack(true); err != nil {
						logger.WithError(err).Error("unable to requeue last chunk")
						break loop
					}

					logger.WithFields(log.Fields{
						"uid":   req.UID,
						"chunk": req.Chunk,
					}).Info("requeue last chunk (#2)")
					break loop
				}

				_ = msg.Nack(false)
				errorsMetric.Counter++
				logger.WithError(err).Error("error while handling encoder")
				continue
			}

			_ = msg.Ack()

			last.request = nil
			last.delivery = nil

			durationMetric := &metric.DurationMetric{
				RowMetric: metric.RowMetric{Name: "nitro_encoder_tasks_duration", Tags: metric.Tags{"provider": e.provider, "hostname": hostname, "uid": req.UID}},
				Duration:  time.Since(taskStarted),
			}
			e.metric.Send(durationMetric.Metric())
		}
	}

	logger.Info("encoder stopped")

	durationMetric := &metric.DurationMetric{
		RowMetric: metric.RowMetric{Name: "nitro_encoder_duration", Tags: metric.Tags{"provider": e.provider, "hostname": hostname}},
		Duration:  time.Since(encoderStarted),
	}
	e.metric.Send(durationMetric.Metric())
}

type result struct {
	ID        int
	Qualities map[int]string
}

func (e *encoder) HandleEncoder(ctx context.Context, req queue.EncoderRequest) error {
	logger.WithFields(log.Fields{
		"uid":   req.UID,
		"chunk": req.Chunk,
	}).Info("receive encoder request")

	encodedFile, err := encode(ctx, req.UID, e.bucket, req.Chunk, req.Params)

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

	logger.WithFields(log.Fields{
		"uid":       req.UID,
		"qualities": encodedFile.Qualities,
	}).Info("send encoder response")

	return nil
}

func encode(ctx context.Context, uid string, bucket storage.Bucket, chunkFilePath string, p []queue.Params) (*result, error) {
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

	ffmpeg := &executor.Cmd{}
	ffmpeg.Add("-hide_banner")

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

		if viper.GetBool("cuda") {
			ffmpeg.Add("-preset", "fast")
		} else {
			ffmpeg.Add("-preset", params.Preset)
			ffmpeg.Add("-crf", strconv.Itoa(params.CRF))
		}

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

	trans := new(transcode.Transcoder)
	err = trans.Initialize(workDir+"/"+fileName, ffmpeg.Command())

	if err != nil {
		return nil, errors.Wrap(err, "transcoder initialization")
	}

	done := trans.Run(true)
	progress := trans.Output()

	for msg := range progress {
		logger.WithFields(log.Fields{
			"current":  msg.CurrentDuration,
			"duration": msg.CompleteDuration,
			"progress": fmt.Sprintf("%05.2f%%", msg.Progress),
			"speed":    msg.Speed,
		}).Debug(msg.CurrentTime)
	}

	err = <-done

	if err != nil {
		return nil, errors.Wrap(err, "transcoding chunk")
	}

	for quality, encodedFilePath := range list {
		filePath := fmt.Sprintf("%s/encoded/%d/%03d.mp4", uid, quality, id)

		if err = util.Upload(bucket, filePath, encodedFilePath, storage.PrivateACL); err != nil {
			return nil, errors.Wrap(err, "encoded chunk storage")
		}

		res.Qualities[quality] = filePath
	}

	return res, nil
}
