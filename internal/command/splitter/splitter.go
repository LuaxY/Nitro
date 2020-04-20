package splitter

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"nitro/internal/command/root"
	"nitro/internal/executor"
	"nitro/internal/metric"
	"nitro/internal/queue"
	"nitro/internal/signal"
	"nitro/internal/storage"
	"nitro/internal/util"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use:   "splitter",
	Short: "Split video into chunks",
	Long:  `Nitro Splitter: split video file into multiple chunks of video and one audio`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("starting splitter")

		cmpt := root.GetComponent(false, true, true, true)

		s := splitter{
			channel: cmpt.Channel,
			bucket:  cmpt.Bucket,
			metric:  cmpt.Metric,
		}

		s.Run()
	},
}

type splitter struct {
	channel queue.Channel
	bucket  storage.Bucket
	metric  metric.Client
}

func (s *splitter) Run() {
	ctx := signal.WatchInterrupt(context.Background(), 10*time.Second)

	log.Info("splitter started")

	go s.metric.Ticker(context.Background(), 1*time.Second)

	hostname, _ := os.Hostname()

	counterMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_splitter_tasks_total", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	gaugeMetric := &metric.GaugeMetric{
		RowMetric: metric.RowMetric{Name: "nitro_splitter_tasks_count", Tags: metric.Tags{"hostname": hostname}},
		Gauge:     0,
	}

	errorsMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_splitter_tasks_errors", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	s.metric.Add(counterMetric)
	s.metric.Add(gaugeMetric)
	s.metric.Add(errorsMetric)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			var req queue.SplitterRequest
			ok, msg, err := s.channel.Consume("splitter.request", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume splitter.request")
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

			if err = s.HandleSplitter(ctx, req); err != nil {
				_ = msg.Nack(false)
				errorsMetric.Counter++
				log.WithError(err).Error("error while handling splitter")
			} else {
				_ = msg.Ack()
			}

			durationMetric := &metric.DurationMetric{
				RowMetric: metric.RowMetric{Name: "nitro_splitter_tasks_duration", Tags: metric.Tags{"hostname": hostname, "uid": req.UID}},
				Duration:  time.Since(started),
			}
			s.metric.Send(durationMetric.Metric())
		}
	}

	log.Info("splitter stopped")
}

func (s *splitter) HandleSplitter(ctx context.Context, req queue.SplitterRequest) error {
	log.WithFields(log.Fields{
		"app":        "splitter",
		"uid":        req.UID,
		"input":      req.Input,
		"chunk_time": req.ChunkTime,
		"video_map":  req.VideoMap,
		"audio_map":  req.AudioMap,
	}).Info("receive splitter request")

	splittedFiles, err := split(ctx, req.UID, s.bucket, s.channel, req.Input, req.ChunkTime, req.VideoMap, req.AudioMap, req.Params)

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

func split(ctx context.Context, uid string, bucket storage.Bucket, channel queue.Channel, masterFilePath string, chunkTime, videoMap, audioMap int, params []queue.Params) (*result, error) {
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
	ffmpeg.Add("-map", "0:"+strconv.Itoa(videoMap))
	ffmpeg.Add("-c:v", "copy")
	ffmpeg.Add("-f", "segment")
	ffmpeg.Add("-segment_time", strconv.Itoa(chunkTime))
	ffmpeg.Add(workDir + "/chunks/%03d.mp4")
	err = exec.Run(ctx, ffmpeg)

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

		chunk := uid + "/chunks/" + info.Name()
		res.Chunks = append(res.Chunks, chunk)

		if err = util.Upload(bucket, chunk, path, storage.PrivateACL); err != nil {
			return err
		}

		if err := channel.Publish("encoder.request", queue.EncoderRequest{
			UID:    uid,
			Chunk:  chunk,
			Params: params,
		}); err != nil {
			return errors.Wrap(err, "unable to publish in encoder.request")
		}

		log.WithFields(log.Fields{
			"app":   "splitter",
			"uid":   uid,
			"chunk": chunk,
		}).Info("send encoder request")

		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to store chunk file")
	}

	// Audio

	ffmpeg = &executor.Cmd{Binary: "ffmpeg"}
	ffmpeg.Add("-i", workDir+"/master.mp4")
	ffmpeg.Add("-hide_banner", "-y", "-dn", "-map_metadata", "-1", "-map_chapters", "-1")
	ffmpeg.Add("-map", "0:"+strconv.Itoa(audioMap)) // TODO multiple audio ?
	ffmpeg.Add("-c:a", "aac")
	ffmpeg.Add("-ac", "2")
	// TODO audio bitrate
	ffmpeg.Add("-f", "mp4")
	ffmpeg.Add(workDir + "/audio.m4a")
	err = exec.Run(ctx, ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to extract audio from master file with ffmpeg")
	}

	if err = util.Upload(bucket, uid+"/audio.m4a", workDir+"/audio.m4a", storage.PublicACL); err != nil {
		return nil, errors.Wrap(err, "unable to store audio file")
	}

	res.Audio = uid + "/audio.m4a"

	return res, nil
}
