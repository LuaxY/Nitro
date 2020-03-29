package watcher

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"trancode/internal/command/root"
	"trancode/internal/database"
	"trancode/internal/metric"
	"trancode/internal/queue"
	"trancode/internal/storage"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use: "watcher",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("starting watcher")

		cmpt := root.GetComponent(true, true, true, true)

		w := watcher{
			db:      cmpt.DB,
			channel: cmpt.Channel,
			bucket:  cmpt.Bucket,
			metric:  cmpt.Metric,
		}

		w.Run()
	},
}

var (
	totalChunksCache map[string]int
)

type watcher struct {
	db      database.Database
	channel queue.Channel
	bucket  storage.Bucket
	metric  metric.Client
}

func (w *watcher) Run() {
	queueList := []string{
		"splitter.request",
		"splitter.response",
		"encoder.request",
		"encoder.response",
		"merger.request",
		"merger.response",
		"packager.request",
		"packager.response",
		"thumbnail.request",
		"thumbnail.response",
	}

	for _, queueName := range queueList {
		log.Debugf("create queue '%s'", queueName)
		_ = w.channel.CreateQueue(queueName)
	}

	go w.metric.Ticker(context.Background(), 1*time.Second)

	totalChunksCache = make(map[string]int)

	var wg sync.WaitGroup
	log.Info("watcher started")

	wg.Add(1)
	go w.watch(wg, "splitter", "splitter.response", &queue.SplitterResponse{}, func(msg interface{}) error {
		splitterResponse, ok := msg.(*queue.SplitterResponse)
		if !ok {
			return errors.New("message is not queue.SplitterResponse")
		}
		return w.HandleSplitter(splitterResponse)
	})

	wg.Add(1)
	go w.watch(wg, "encoder", "encoder.response", &queue.EncoderResponse{}, func(msg interface{}) error {
		encoderResponse, ok := msg.(*queue.EncoderResponse)
		if !ok {
			return errors.New("message is not queue.EncoderResponse")
		}
		return w.HandleEncoder(encoderResponse)
	})

	wg.Add(1)
	go w.watch(wg, "merger", "merger.response", &queue.MergerResponse{}, func(msg interface{}) error {
		mergerResponse, ok := msg.(*queue.MergerResponse)
		if !ok {
			return errors.New("message is not queue.MergerResponse")
		}
		return w.HandleMerger(mergerResponse)
	})

	wg.Add(1)
	go w.watch(wg, "packager", "packager.response", &queue.PackagerResponse{}, func(msg interface{}) error {
		packagerResponse, ok := msg.(*queue.PackagerResponse)
		if !ok {
			return errors.New("message is not queue.PackagerResponse")
		}
		return w.HandlePackager(packagerResponse)
	})

	wg.Wait()
	log.Info("watcher ended")
}

func (w *watcher) watch(wg sync.WaitGroup, taskName string, queueName string, msg interface{}, callback func(msg interface{}) error) {
	defer wg.Done()
	log.Info("start watching ", taskName)

	hostname, _ := os.Hostname()

	counterMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_watcher_tasks_total", Tags: metric.Tags{"hostname": hostname, "task": taskName}},
		Counter:   0,
	}

	gaugeMetric := &metric.GaugeMetric{
		RowMetric: metric.RowMetric{Name: "nitro_watcher_tasks_count", Tags: metric.Tags{"hostname": hostname, "task": taskName}},
		Gauge:     0,
	}

	w.metric.Add(counterMetric)
	w.metric.Add(gaugeMetric)

	for {
		ok, err := w.channel.Consume(queueName, msg)

		if err != nil {
			log.WithError(err).Error("unable to consume ", queueName)
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

		if err := callback(msg); err != nil {
			log.WithError(err).Error("error while handling ", taskName)
		}

		uid := "unknown"
		value := reflect.ValueOf(msg).Elem().FieldByName("UID")

		if value.IsValid() {
			uid = value.String()
		}

		durationMetric := &metric.DurationMetric{
			RowMetric: metric.RowMetric{Name: "nitro_watcher_tasks_duration", Tags: metric.Tags{"hostname": hostname, "task": taskName, "uid": uid}},
			Duration:  time.Since(started),
		}
		w.metric.Send(durationMetric.Metric())
	}
}

func (w *watcher) HandleSplitter(req *queue.SplitterResponse) error {
	log.WithFields(log.Fields{
		"watch":        "splitter",
		"uid":          req.UID,
		"total_chunks": len(req.Chunks),
	}).Info("receive splitter response")

	if err := w.db.Set(req.UID+".total", strconv.Itoa(req.TotalChunks), 24*time.Hour); err != nil {
		return errors.Wrap(err, "unable to consume splitter.response")
	}

	for _, chunk := range req.Chunks {
		if err := w.channel.Publish("encoder.request", queue.EncoderRequest{
			UID:    req.UID,
			Chunk:  chunk,
			Params: req.Params,
		}); err != nil {
			return errors.Wrap(err, "unable to publish in encoder.request")
		}

		log.WithFields(log.Fields{
			"watch": "splitter",
			"uid":   req.UID,
			"chunk": chunk,
		}).Info("send encoder request")
	}

	return nil
}

func (w *watcher) HandleEncoder(req *queue.EncoderResponse) error {
	log.WithFields(log.Fields{
		"watch":     "encoder",
		"uid":       req.UID,
		"qualities": req.Qualities,
	}).Info("receive encoder response")

	totalChunks, ok := totalChunksCache[req.UID]

	if !ok {
		totalString, err := w.db.Get(req.UID + ".total")

		if err != nil {
			return errors.Wrapf(err, "unable to get total chunks for '%s'", req.UID)
		}

		totalChunks, err = strconv.Atoi(totalString)

		if err != nil {
			return errors.Wrapf(err, "unable to convert string '%s' to int", totalString)
		}
	}

	store := make(map[int]map[int]string) // TODO rename

	// TODO potential (not really) data lost if 2 encoder response at same time, refactor this part to use 1 key for each chunk
	// maybe use redis INC a dn compare to total
	if listData, err := w.db.Get(req.UID); err == nil {
		if err = json.Unmarshal([]byte(listData), &store); err != nil {
			return errors.Wrapf(err, "unable to decode '%s' data", req.UID)
		}
	}

	store[req.ID] = req.Qualities
	listData, err := json.Marshal(store)

	if err != nil {
		return errors.Wrapf(err, "unable to marshal '%s' data", req.UID)
	}

	if err = w.db.Set(req.UID, string(listData), 24*time.Hour); err != nil {
		return errors.Wrapf(err, "unable to store data for '%s'", req.UID)
	}

	if len(store) >= totalChunks {
		if err = w.channel.Publish("merger.request", queue.MergerRequest{
			UID:    req.UID,
			Chunks: store,
		}); err != nil {
			return errors.Wrap(err, "unable to publish in merger.request")
		}

		log.WithFields(log.Fields{
			"watch": "encoder",
			"uid":   req.UID,
		}).Info("send merger request")
	}

	return nil
}

func (w *watcher) HandleMerger(req *queue.MergerResponse) error {
	log.WithFields(log.Fields{
		"watch":     "merger",
		"uid":       req.UID,
		"qualities": req.Qualities,
	}).Info("receive merger response")

	if err := w.channel.Publish("packager.request", queue.PackagerRequest{
		UID:       req.UID,
		Qualities: req.Qualities,
	}); err != nil {
		return errors.Wrap(err, "unable to publish in packager.request")
	}

	log.WithFields(log.Fields{
		"watch":     "merger",
		"uid":       req.UID,
		"qualities": req.Qualities,
	}).Info("send packager request")

	return nil
}

func (w *watcher) HandlePackager(req *queue.PackagerResponse) error {
	log.WithFields(log.Fields{
		"watch": "packager",
		"uid":   req.UID,
	}).Info("receive packager response")

	_ = w.bucket.Delete(req.UID + "/chunks")
	_ = w.bucket.Delete(req.UID + "/encoded")

	return nil
}
