package root

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"trancode/internal/database"
	"trancode/internal/metric"
	"trancode/internal/queue"
	"trancode/internal/storage"
)

var Cmd = &cobra.Command{
	Use:   "transcoder",
	Short: "WatchNow Transcoder",
	Long:  `WatchNow Transcoder`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
}

func Execute() {
	log.SetLevel(log.DebugLevel)

	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	Cmd.PersistentFlags().String("storage", "/data", "Storage bucket")

	Cmd.PersistentFlags().String("aws-bucket", "", "AWS bucket")
	Cmd.PersistentFlags().String("aws-region", "", "AWS region")
	Cmd.PersistentFlags().String("aws-endpoint", "", "AWS endpoint")
	Cmd.PersistentFlags().String("aws-id", "", "AWS id")
	Cmd.PersistentFlags().String("aws-secret", "", "AWS secret")

	Cmd.PersistentFlags().String("amqp", "amqp://guest:guest@rabbitmq:5672/", "RabbitMQ AMQP URL")

	Cmd.PersistentFlags().String("redis", "redis:6379", "Redis endpoint")
	Cmd.PersistentFlags().String("redis-password", "", "Redis password")

	Cmd.PersistentFlags().String("influxdb", "influxdb:9999", "InfluxDB endpoint")
	Cmd.PersistentFlags().String("influxdb-token", "", "InfluxDB token")
	Cmd.PersistentFlags().String("influxdb-bucket", "", "InfluxDB bucket")
	Cmd.PersistentFlags().String("influxdb-org", "", "InfluxDB organization")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(Cmd.PersistentFlags()); err != nil {
		log.WithError(err).Fatal("flag biding failed")
	}
}

type Component struct {
	DB      database.Database
	Channel queue.Channel
	Bucket  storage.Bucket
	Metric  metric.Client
}

func GetComponent(loadDB, loadQueue, loadStorage, loadeMetric bool) *Component {
	component := &Component{}

	if loadDB {
		redisAddr := viper.GetString("redis")
		db, err := database.NewRedis(&redis.Options{
			Addr:     redisAddr,
			Password: viper.GetString("redis-password"),
		})

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to database '%s'", redisAddr)
		}

		log.Infof("connected to database '%s'", redisAddr)
		component.DB = db
	}

	if loadQueue {
		amqp := viper.GetString("amqp")
		channel, err := queue.NewRabbitMQ(context.Background(), amqp)

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to queue '%s'", amqp)
		}

		log.Infof("connected to queue '%s'", amqp)
		component.Channel = channel
	}

	if loadStorage {
		//bucketName := viper.GetString("storage")
		//bucket, err := storage.NewLocal(context.Background(), bucketName)

		bucketName := viper.GetString("aws-bucket")
		bucket, err := storage.NewS3(context.Background(), bucketName, &aws.Config{
			Endpoint:    aws.String(viper.GetString("aws-endpoint")),
			Region:      aws.String(viper.GetString("aws-region")),
			Credentials: credentials.NewStaticCredentials(viper.GetString("aws-id"), viper.GetString("aws-secret"), ""),
		})

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to storage '%s'", bucketName)
		}

		log.Infof("connected to storage '%s'", bucketName)
		component.Bucket = bucket
	}

	if loadeMetric {
		influxDbAddr := viper.GetString("influxdb")
		metricClient, err := metric.NewInfluxdb(metric.InfluxdbConfig{
			Addr:   influxDbAddr,
			Token:  viper.GetString("influxdb-token"),
			Bucket: viper.GetString("influxdb-bucket"),
			Org:    viper.GetString("influxdb-org"),
		})

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to metrics '%s'", influxDbAddr)
		}

		log.Infof("connected to metrics '%s'", influxDbAddr)
		component.Metric = metricClient
	}

	return component
}
