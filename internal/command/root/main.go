package root

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(Cmd.PersistentFlags()); err != nil {
		log.WithError(err).Fatal("flag biding failed")
	}
}
