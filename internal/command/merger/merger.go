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
	Use:   "merger",
	Short: "Merge video chunks",
	Long:  `Nitro Merger: merge video chunks for each qualities`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("starting merger")

		cmpt := root.GetComponent(false, true, true, true)

		m := merger{
			channel: cmpt.Channel,
			bucket:  cmpt.Bucket,
			metric:  cmpt.Metric,
		}

		m.Run()
	},
}

type merger struct {
	channel queue.Channel
	bucket  storage.Bucket
	metric  metric.Client
}

func (m *merger) Run() {
	ctx := signal.WatchInterrupt(context.Background(), 10*time.Second)

	log.Info("merger started")

	go m.metric.Ticker(context.Background(), 1*time.Second)

	hostname, _ := os.Hostname()

	counterMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_merger_tasks_total", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	gaugeMetric := &metric.GaugeMetric{
		RowMetric: metric.RowMetric{Name: "nitro_merger_tasks_count", Tags: metric.Tags{"hostname": hostname}},
		Gauge:     0,
	}

	m.metric.Add(counterMetric)
	m.metric.Add(gaugeMetric)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			var req queue.MergerRequest
			ok, err := m.channel.Consume("merger.request", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume merger.request")
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

			if err = m.HandleMerger(ctx, req); err != nil {
				log.WithError(err).Error("error while handling merger")
			}

			durationMetric := &metric.DurationMetric{
				RowMetric: metric.RowMetric{Name: "nitro_merger_tasks_duration", Tags: metric.Tags{"hostname": hostname, "uid": req.UID}},
				Duration:  time.Since(started),
			}
			m.metric.Send(durationMetric.Metric())
		}
	}
}

func (m *merger) HandleMerger(ctx context.Context, req queue.MergerRequest) error {
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
		_, err := merge(ctx, req.UID, m.bucket, quality, chunks)

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

func merge(ctx context.Context, uid string, bucket storage.Bucket, quality int, chunks map[int]string) (*result, error) {
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
	err = exec.Run(ctx, ffmpeg)

	if err != nil {
		return nil, errors.Wrap(err, "unable to merge chunk files with ffmpeg")
	}

	finalPath := fmt.Sprintf("%s/%d.mp4", uid, quality)

	if err = util.Upload(bucket, finalPath, workDir+"/merged.mp4", storage.PrivateACL); err != nil {
		return nil, errors.Wrap(err, "unable to store merged file")
	}

	res.Path = finalPath

	return res, nil
}
