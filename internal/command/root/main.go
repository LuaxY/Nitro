package root

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gocloud.dev/gcp"

	"nitro/internal/database"
	"nitro/internal/metric"
	"nitro/internal/queue"
	"nitro/internal/storage"
)

var Cmd = &cobra.Command{
	Use:   "nitro",
	Short: "Nitro",
	Long:  `Nitro - Distributed video encoder pipeline`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
}

func Execute() {
	if viper.GetBool("json-log") {
		log.SetFormatter(&log.JSONFormatter{})
	}

	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)

	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	Cmd.PersistentFlags().Bool("json-log", false, "Format log into JSON")

	Cmd.PersistentFlags().String("storage", "/data", "Storage bucket")

	Cmd.PersistentFlags().String("aws-bucket", "", "AWS bucket")
	Cmd.PersistentFlags().String("aws-region", "", "AWS region")
	Cmd.PersistentFlags().String("aws-endpoint", "", "AWS endpoint")
	Cmd.PersistentFlags().String("aws-id", "", "AWS id")
	Cmd.PersistentFlags().String("aws-secret", "", "AWS secret")

	Cmd.PersistentFlags().String("gcs-bucket", "", "GCS bucket")

	Cmd.PersistentFlags().String("rabbitmq-protocol", "amqp", "RabbitMQ host")
	Cmd.PersistentFlags().String("rabbitmq-host", "rabbitmq", "RabbitMQ host")
	Cmd.PersistentFlags().Int("rabbitmq-port", 5672, "RabbitMQ port")
	Cmd.PersistentFlags().String("rabbitmq-user", "guest", "RabbitMQ username")
	Cmd.PersistentFlags().String("rabbitmq-pass", "guest", "RabbitMQ password")

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

func GetComponent(loadDB, loadQueue, loadStorage, loadMetric bool) *Component {
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
		rabbitMQProtocol := viper.GetString("rabbitmq-protocol")
		rabbitMQHost := viper.GetString("rabbitmq-host")
		rabbitMQPort := viper.GetInt("rabbitmq-port")
		rabbitMQUser := viper.GetString("rabbitmq-user")
		rabbitMQPass := viper.GetString("rabbitmq-pass")

		amqp := fmt.Sprintf("%s://%s:%s@%s:%d/", rabbitMQProtocol, rabbitMQUser, rabbitMQPass, rabbitMQHost, rabbitMQPort)

		channel, err := queue.NewRabbitMQ(context.Background(), amqp)

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to queue '%s'", strings.Replace(amqp, rabbitMQPass, "*******", -1))
		}

		log.Infof("connected to queue '%s'", strings.Replace(amqp, rabbitMQPass, "*******", -1))
		component.Channel = channel
	}

	if loadStorage {
		//bucketName := viper.GetString("storage")
		//bucket, err := storage.NewLocal(context.Background(), bucketName)

		/*bucketName := viper.GetString("aws-bucket")
		bucket, err := storage.NewS3(context.Background(), bucketName, &aws.Config{
			Endpoint:    aws.String(viper.GetString("aws-endpoint")),
			Region:      aws.String(viper.GetString("aws-region")),
			Credentials: credentials.NewStaticCredentials(viper.GetString("aws-id"), viper.GetString("aws-secret"), ""),
		})*/

		creds, err := gcp.DefaultCredentials(context.Background())

		if err != nil {
			log.WithError(err).Fatalf("unable to find default GCS credentials")
		}

		client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))

		if err != nil {
			log.WithError(err).Fatalf("unable to create GCS HTTP client")
		}

		bucketName := viper.GetString("gcs-bucket")
		bucket, err := storage.NewGCS(context.Background(), bucketName, client)

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to storage '%s'", bucketName)
		}

		log.Infof("connected to storage '%s'", bucketName)
		component.Bucket = bucket
	}

	if loadMetric {
		/*metricEndpoint := viper.GetString("influxdb")
		metricClient, err := metric.NewInfluxdb(metric.InfluxdbConfig{
			Addr:   metricEndpoint,
			Token:  viper.GetString("influxdb-token"),
			Bucket: viper.GetString("influxdb-bucket"),
			Org:    viper.GetString("influxdb-org"),
		})

		if err != nil {
			log.WithError(err).Fatalf("unable to connect to metrics '%s'", metricEndpoint)
		}*/

		metricEndpoint := "null"
		metricClient := &metric.Null{}

		log.Infof("connected to metrics '%s'", metricEndpoint)
		component.Metric = metricClient
	}

	return component
}
