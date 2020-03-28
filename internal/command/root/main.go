package root

import (
	"fmt"
	"os"
	"strings"

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
	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	Cmd.PersistentFlags().String("storage", "/data", "Storage path")
	Cmd.PersistentFlags().String("amqp", "amqp://guest:guest@rabbitmq:5672/", "RabbitMQ AMQP URL")
	Cmd.PersistentFlags().String("redis", "redis:6379", "Redis endpoint")
	Cmd.PersistentFlags().String("redis-password", "", "Redis password")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(Cmd.PersistentFlags()); err != nil {
		//logger.WithError(err).Fatal("flag biding failed") // TODO
		panic(0)
	}
}
