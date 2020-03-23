package watcher

import (
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"trancode/internal/command/root"
	"trancode/internal/database"
	"trancode/internal/queue"
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

	_ = channel.CreateQueue("splitter.request")
	_ = channel.CreateQueue("encoder.request")
	_ = channel.CreateQueue("encoder.response")
	_ = channel.CreateQueue("merger.request")

	for {
		data, ok, err := channel.Consume("encoder.response")

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		var req queue.EncoderResponse

		err = yaml.Unmarshal(data, &req)

		if err != nil {
			log.Fatal(err)
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

		// TODO
		if len(store) >= 5 {
			yml, err := yaml.Marshal(queue.MergerRequest{
				UID:    req.UID,
				Chunks: store,
			})

			if err != nil {
				log.Fatal(err)
			}

			if err = channel.Publish("merger.request", yml); err != nil {
				log.Fatal(err)
			}
		}
	}
}
