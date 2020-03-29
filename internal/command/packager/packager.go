package packager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
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
	Use: "packager",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("starting packager")

		cmpt := root.GetComponent(false, true, true, true)

		p := packager{
			channel: cmpt.Channel,
			bucket:  cmpt.Bucket,
			metric:  cmpt.Metric,
		}

		p.Run()
	},
}

type packager struct {
	channel queue.Channel
	bucket  storage.Bucket
	metric  metric.Client
}

func (p *packager) Run() {
	log.Info("packager started")

	go p.metric.Ticker(context.Background(), 1*time.Second)

	hostname, _ := os.Hostname()

	counterMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_packager_tasks_total", Tags: metric.Tags{"hostname": hostname}},
		Counter:   0,
	}

	gaugeMetric := &metric.GaugeMetric{
		RowMetric: metric.RowMetric{Name: "nitro_packager_tasks_count", Tags: metric.Tags{"hostname": hostname}},
		Gauge:     0,
	}

	p.metric.Add(counterMetric)
	p.metric.Add(gaugeMetric)

	for {
		var req queue.PackagerRequest
		ok, err := p.channel.Consume("packager.request", &req)

		if err != nil {
			log.WithError(err).Error("unable to consume packager.request")
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

		if err = p.HandlePackager(req); err != nil {
			log.WithError(err).Error("error while handling packager")
		}

		durationMetric := &metric.DurationMetric{
			RowMetric: metric.RowMetric{Name: "nitro_packager_tasks_duration", Tags: metric.Tags{"hostname": hostname, "uid": req.UID}},
			Duration:  time.Since(started),
		}
		p.metric.Send(durationMetric.Metric())
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