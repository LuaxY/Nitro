package watcher

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"nitro/internal/command/root"
	"nitro/internal/database"
	"nitro/internal/metric"
	"nitro/internal/queue"
	"nitro/internal/signal"
	"nitro/internal/storage"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use:   "watcher",
	Short: "Orchestrate the pipeline",
	Long:  `Nitro Watcher: orchestrator of pipeline, watch response to create next component request`,
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
	ctx := signal.WatchInterrupt(context.Background(), 10*time.Second)

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

	log.Info("watcher started")

	// TODO worker pool instead of sync goroutine for each task type
	// TODO send task info /w callback in chan
	// TODO start X worker who get work task from chan

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go w.watch(ctx, wg, "splitter", "splitter.response", &queue.SplitterResponse{}, func(msg interface{}) error {
		splitterResponse, ok := msg.(*queue.SplitterResponse)
		if !ok {
			return errors.New("message is not queue.SplitterResponse")
		}
		return w.HandleSplitter(splitterResponse)
	})

	wg.Add(1)
	go w.watch(ctx, wg, "encoder", "encoder.response", &queue.EncoderResponse{}, func(msg interface{}) error {
		encoderResponse, ok := msg.(*queue.EncoderResponse)
		if !ok {
			return errors.New("message is not queue.EncoderResponse")
		}
		return w.HandleEncoder(encoderResponse)
	})

	wg.Add(1)
	go w.watch(ctx, wg, "merger", "merger.response", &queue.MergerResponse{}, func(msg interface{}) error {
		mergerResponse, ok := msg.(*queue.MergerResponse)
		if !ok {
			return errors.New("message is not queue.MergerResponse")
		}
		return w.HandleMerger(mergerResponse)
	})

	wg.Add(1)
	go w.watch(ctx, wg, "packager", "packager.response", &queue.PackagerResponse{}, func(msg interface{}) error {
		packagerResponse, ok := msg.(*queue.PackagerResponse)
		if !ok {
			return errors.New("message is not queue.PackagerResponse")
		}
		return w.HandlePackager(packagerResponse)
	})

	wg.Wait()
	log.Info("watcher stopped")
}

func (w *watcher) watch(ctx context.Context, wg *sync.WaitGroup, taskName string, queueName string, response interface{}, callback func(msg interface{}) error) {
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

	errorsMetric := &metric.CounterMetric{
		RowMetric: metric.RowMetric{Name: "nitro_watcher_tasks_errors", Tags: metric.Tags{"hostname": hostname, "task": taskName}},
		Counter:   0,
	}

	w.metric.Add(counterMetric)
	w.metric.Add(gaugeMetric)
	w.metric.Add(errorsMetric)

loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("stop watching ", taskName)
			break loop
		default:
			ok, msg, err := w.channel.Consume(queueName, response)

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

			if err := callback(response); err != nil {
				_ = msg.Nack(false)
				errorsMetric.Counter++
				log.WithError(err).Error("error while handling ", taskName)
			} else {
				_ = msg.Ack()
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
}

// TODO remove or improve
var start time.Time

func (w *watcher) HandleSplitter(req *queue.SplitterResponse) error {
	log.WithFields(log.Fields{
		"watch":        "splitter",
		"uid":          req.UID,
		"total_chunks": len(req.Chunks),
		"langs":        req.Langs,
	}).Info("receive splitter response")

	if err := w.db.Set(req.UID+".total", strconv.Itoa(req.TotalChunks), 24*time.Hour); err != nil {
		return errors.Wrap(err, "store total chunks")
	}

	if err := w.db.Set(req.UID+".langs", strings.Join(req.Langs, ","), 24*time.Hour); err != nil {
		return errors.Wrap(err, "store lang list")
	}

	// TODO remove
	start = time.Now()
	log.Debug("STARTED ", req.UID, " ", start)

	/*for _, chunk := range req.Chunks {
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
	}*/

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
			// FIXME error trigger if at least one encoder finish before splitter
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

	langs, err := w.db.Get(req.UID + ".langs")

	if err != nil {
		return errors.Wrapf(err, "unable to get langs for '%s'", req.UID)
	}

	if err := w.channel.Publish("packager.request", queue.PackagerRequest{
		UID:       req.UID,
		Qualities: req.Qualities,
		Langs:     strings.Split(langs, ","),
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

	log.Debug("FINISHED ", req.UID, " ", time.Now(), " ", time.Since(start)) // TODO remove

	_ = w.bucket.Delete(context.Background(), req.UID+"/chunks")
	_ = w.bucket.Delete(context.Background(), req.UID+"/encoded")

	_ = w.db.Delete(req.UID)
	_ = w.db.Delete(req.UID + ".total")

	return nil
}
