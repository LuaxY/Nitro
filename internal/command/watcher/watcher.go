package watcher

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/cobra"

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
		Run()
	},
}

func Run() {
	db, err := database.NewRedis(&redis.Options{
		Addr: root.Cmd.Flag("redis").Value.String(),
	})

	if err != nil {
		log.Fatal(err)
	}

	channel, err := queue.NewRabbitMQ(root.Cmd.Flag("amqp").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	bucket, err := storage.NewLocal(context.Background(), root.Cmd.Flag("storage").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	_ = channel.CreateQueue("splitter.request")
	_ = channel.CreateQueue("splitter.response")
	_ = channel.CreateQueue("encoder.request")
	_ = channel.CreateQueue("encoder.response")
	_ = channel.CreateQueue("merger.request")
	_ = channel.CreateQueue("merger.response")
	_ = channel.CreateQueue("packer.request")
	_ = channel.CreateQueue("packer.response")
	_ = channel.CreateQueue("thumbnail.request")
	_ = channel.CreateQueue("thumbnail.response")

	totalChunksCache := make(map[string]int)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var req queue.SplitterResponse
			ok, err := channel.Consume("splitter.response", &req)

			if err != nil {
				log.Fatal(err)
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err = db.Set(req.UID+".total", strconv.Itoa(req.TotalChunks)); err != nil {
				log.Fatal(err)
			}

			for _, chunk := range req.Chunks {
				if err = channel.Publish("encoder.request", queue.EncoderRequest{
					UID:    req.UID,
					Chunk:  chunk,
					Params: req.Params,
				}); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var req queue.EncoderResponse
			ok, err := channel.Consume("encoder.response", &req)

			if err != nil {
				log.Fatal(err)
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			totalChunks, ok := totalChunksCache[req.UID]

			if !ok {
				totalString, err := db.Get(req.UID + ".total")

				if err != nil {
					log.Fatal(err)
				}

				totalChunks, err = strconv.Atoi(totalString)

				if err != nil {
					log.Fatal(err)
				}
			}

			store := make(map[int]map[int]string) // TODO rename

			if listData, err := db.Get(req.UID); err == nil {
				if err = json.Unmarshal([]byte(listData), &store); err != nil {
					log.Fatal(err)
				}
			}

			store[req.ID] = req.Qualities
			listData, err := json.Marshal(store)

			if err != nil {
				log.Fatal(err)
			}

			if err = db.Set(req.UID, string(listData)); err != nil {
				log.Fatal(err)
			}

			if len(store) >= totalChunks {
				if err = channel.Publish("merger.request", queue.MergerRequest{
					UID:    req.UID,
					Chunks: store,
				}); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var req queue.MergerResponse
			ok, err := channel.Consume("merger.response", &req)

			if err != nil {
				log.Fatal(err)
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			if err = channel.Publish("packer.request", queue.PackagerRequest{
				UID:       req.UID,
				Qualities: req.Qualities,
			}); err != nil {
				log.Fatal(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var req queue.PackagerResponse
			ok, err := channel.Consume("packer.response", &req)

			if err != nil {
				log.Fatal(err)
			}

			if !ok {
				time.Sleep(5 * time.Second)
				continue
			}

			_ = bucket.Delete(req.UID + "/chunks")
			_ = bucket.Delete(req.UID + "/encoded")
		}
	}()

	wg.Wait()
}
