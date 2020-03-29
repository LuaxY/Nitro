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
	Use: "splitter",
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

	s.metric.Add(counterMetric)
	s.metric.Add(gaugeMetric)

	for {
		var req queue.SplitterRequest
		ok, err := s.channel.Consume("splitter.request", &req)

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

		if err = s.HandleSplitter(req); err != nil {
			log.WithError(err).Error("error while handling splitter")
		}

		durationMetric := &metric.DurationMetric{
			RowMetric: metric.RowMetric{Name: "nitro_splitter_tasks_duration", Tags: metric.Tags{"hostname": hostname, "uid": req.UID}},
			Duration:  time.Since(started),
		}
		s.metric.Send(durationMetric.Metric())
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
		return util.Upload(bucket, uid+"/chunks/"+info.Name(), path, storage.PrivateACL)
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

	if err = util.Upload(bucket, uid+"/audio.m4a", workDir+"/audio.m4a", storage.PublicACL); err != nil {
		return nil, errors.Wrap(err, "unable to store audio file")
	}

	res.Audio = uid + "/audio.m4a"

	return res, nil
}
