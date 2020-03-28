package watcher

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"trancode/internal/command/root"
	"trancode/internal/database"
	"trancode/internal/queue"
	"trancode/internal/storage"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use: "watcher",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		log.Info("starting watcher")

		redisAddr := viper.GetString("redis")
		db, err := database.NewRedis(&redis.Options{
			Addr:     redisAddr,
			Password: viper.GetString("redis-password"),
		})

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to database '%s'", redisAddr)
		}

		log.Infof("connected to database '%s'", redisAddr)

		amqp := viper.GetString("amqp")
		channel, err := queue.NewRabbitMQ(context.Background(), amqp)

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to queue '%s'", amqp)
		}

		log.Infof("connected to queue '%s'", amqp)

		//bucketName := viper.GetString("storage")
		//bucket, err := storage.NewLocal(context.Background(), bucketName)

		bucketName := viper.GetString("aws-bucket")
		bucket, err := storage.NewS3(context.Background(), bucketName, &aws.Config{
			Endpoint:    aws.String(viper.GetString("aws-endpoint")),
			Region:      aws.String(viper.GetString("aws-region")),
			Credentials: credentials.NewStaticCredentials(viper.GetString("aws-id"), viper.GetString("aws-secret"), ""),
		})

		if err != nil {
			log.Infof("connected to storage '%s'", bucketName)
		}

		log.Infof("connected to storage '%s'", bucketName)

		w := watcher{
			db:      db,
			channel: channel,
			bucket:  bucket,
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

	totalChunksCache = make(map[string]int)

	var wg sync.WaitGroup

	log.Info("watcher started")

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("start watching splitter")
		for {
			var req queue.SplitterResponse
			ok, err := w.channel.Consume("splitter.response", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume splitter.response")
				time.Sleep(5 * time.Second)
				continue
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err := w.HandleSplitter(req); err != nil {
				log.WithError(err).Error("error while handling splitter")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("start watching encoder")
		for {
			var req queue.EncoderResponse
			ok, err := w.channel.Consume("encoder.response", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume encoder.response")
				time.Sleep(5 * time.Second)
				continue
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err := w.HandleEncoder(req); err != nil {
				log.WithError(err).Error("error while handling encoder")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("start watching merger")
		for {
			var req queue.MergerResponse
			ok, err := w.channel.Consume("merger.response", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume merger.response")
				time.Sleep(5 * time.Second)
				continue
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err := w.HandleMerger(req); err != nil {
				log.WithError(err).Error("error while handling merger")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("start watching packager")
		for {
			var req queue.PackagerResponse
			ok, err := w.channel.Consume("packager.response", &req)

			if err != nil {
				log.WithError(err).Error("unable to consume packager.response")
				time.Sleep(5 * time.Second)
				continue
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err := w.HandlePackager(req); err != nil {
				log.WithError(err).Error("error while handling packager")
			}
		}
	}()

	wg.Wait()

	log.Info("watcher ended")
}

func (w *watcher) HandleSplitter(req queue.SplitterResponse) error {
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

func (w *watcher) HandleEncoder(req queue.EncoderResponse) error {
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

func (w *watcher) HandleMerger(req queue.MergerResponse) error {
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

func (w *watcher) HandlePackager(req queue.PackagerResponse) error {
	log.WithFields(log.Fields{
		"watch": "packager",
		"uid":   req.UID,
	}).Info("receive packager response")

	_ = w.bucket.Delete(req.UID + "/chunks")
	_ = w.bucket.Delete(req.UID + "/encoded")

	return nil
}
