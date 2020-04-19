package autoscaler

import (
	"context"
	"fmt"
	"time"

	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"nitro/internal/cloud"
	"nitro/internal/command/root"
	"nitro/internal/metric"
	"nitro/internal/signal"
)

func init() {
	root.Cmd.AddCommand(cmd)

	cmd.PersistentFlags().Int("max-instances", 5, "Maximum number of instances")

	cmd.PersistentFlags().String("gcp-project", "", "GCP project")
	cmd.PersistentFlags().String("gcp-zone", "us-central1-a", "GCP zone")
	cmd.PersistentFlags().String("gcp-group", "nitro-encoders", "GCP group")
	cmd.PersistentFlags().String("gcp-prefix", "nitro-encoder-", "GCP instance name prefix")
	cmd.PersistentFlags().String("gcp-machine-type", "n1-standard-1", "GCP machine type")
	cmd.PersistentFlags().String("gcp-template", "", "GCP image template")
	cmd.PersistentFlags().Bool("gcp-preemtible", true, "GCP preemtible instance")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		log.WithError(err).Fatal("flag biding failed")
	}
}

var cmd = &cobra.Command{
	Use:   "autoscaler",
	Short: "Scale number of encoder instance",
	Long:  `Nitro AutoScaler: scale number of encoder instances by watching number of encoding request in queue`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("starting autoscaler")

		cmpt := root.GetComponent(false, false, false, true)

		as := autoscaler{
			metric: cmpt.Metric,
		}

		as.Run()
	},
}

type autoscaler struct {
	metric metric.Client
}

func (w *autoscaler) Run() {
	ctx := signal.WatchInterrupt(context.Background(), 10*time.Second)

	gcpProject := viper.GetString("gcp-project")
	gcpZone := viper.GetString("gcp-zone")
	gcpGroup := viper.GetString("gcp-group")
	provider, err := cloud.NewGCP(ctx, gcpProject, gcpZone, gcpGroup)

	if err != nil {
		log.WithError(err).Fatal("cloud provider")
	}

	log.WithFields(log.Fields{
		"project": gcpProject,
		"zone":    gcpZone,
		"group":   gcpGroup,
	}).Info("connected to GCP")

	rabbitMQProtocol := viper.GetString("rabbitmq-protocol")
	rabbitMQHost := viper.GetString("rabbitmq-host")

	rabbitMQURL := fmt.Sprintf("%s://%s", rabbitMQProtocol, rabbitMQHost)
	rmqc, err := rabbithole.NewClient(rabbitMQURL, viper.GetString("rabbitmq-user"), viper.GetString("rabbitmq-pass"))

	if err != nil {
		log.WithError(err).Fatal("rabbitmq admin client")
	}

	log.Infof("connected to RabbitMQ admin '%s'", rabbitMQURL)

	MaxInstance := viper.GetInt("max-instances")
	log.Infof("maximum number of instances: %d", MaxInstance)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Info("autoscaler started")

loop:
	for {
		go func() {
			queueInfo, err := rmqc.GetQueue("/", "encoder.request")

			if err != nil {
				log.WithError(err).Error("get queue info")
				return
			}

			count, err := provider.Count(ctx)

			if err != nil {
				log.WithError(err).Error("count instance")
				return
			}

			// TODO metric
			log.WithFields(log.Fields{
				"instances": count,
				"ready":     queueInfo.MessagesReady,
				"unacked":   queueInfo.MessagesUnacknowledged,
				"total":     queueInfo.Messages,
			}).Debug("count")

			if queueInfo.MessagesReady > 0 {
				if count < MaxInstance {
					nbInstances := MaxInstance

					if queueInfo.MessagesReady < MaxInstance {
						nbInstances = queueInfo.MessagesReady
					}

					if nbInstances != count {
						for i := 0; i < nbInstances-count; i++ {
							_, err = provider.AddInstance(ctx, viper.GetString("gcp-prefix"), viper.GetString("gcp-machine-type"), viper.GetString("gcp-template"), viper.GetBool("gcp-preemtible"))

							if err != nil {
								log.WithError(err).Fatal("increase instance number")
							}
						}

						log.WithFields(log.Fields{
							"previous": count,
							"current":  nbInstances,
						}).Info("increase number of instances")
					}
				}
			} else if queueInfo.Messages == 0 {
				if count > 0 {
					if err = provider.DeleteAll(ctx); err != nil {
						log.WithError(err).Fatal("delete all instances")
					}

					log.WithFields(log.Fields{
						"previous": count,
						"current":  0,
					}).Info("decrease number of instances")
				}
			}
		}()

		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			continue
		}
	}

	log.Info("autoscaler stopped")
}
