package metric

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-client-go"
	log "github.com/sirupsen/logrus"
)

type influx struct {
	client  *influxdb.Client
	bucket  string
	org     string
	metrics []Metric
}

type InfluxdbConfig struct {
	Addr   string
	Token  string
	Bucket string
	Org    string
}

type Fields map[string]interface{}

type Tags map[string]string

func NewInfluxdb(config InfluxdbConfig) (*influx, error) {
	client, err := influxdb.New(config.Addr, config.Token)

	if err != nil {
		return nil, err
	}

	return &influx{client: client, bucket: config.Bucket, org: config.Org}, nil
}

func (i *influx) Add(metric Metric) {
	i.metrics = append(i.metrics, metric)
}

func (i *influx) Send(metrics ...influxdb.Metric) {
	if _, err := i.client.Write(context.Background(), i.bucket, i.org, metrics...); err != nil {
		log.WithError(err).Debug("unable to send metrics:")
		for _, metric := range metrics {
			log.WithFields(log.Fields{
				"name":   metric.Name(),
				"tags":   tagsMap(metric.TagList()),
				"fields": fieldsMap(metric.FieldList()),
			}).Debug("metric not send")
		}
	}
}

func (i *influx) Ticker(ctx context.Context, duration time.Duration) {
	ticker := time.NewTicker(duration)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			metrics := make([]influxdb.Metric, len(i.metrics))
			for i, metric := range i.metrics {
				metrics[i] = metric.Metric()
			}
			i.Send(metrics...)
		}
	}
}

func tagsMap(tags []*influxdb.Tag) (t Tags) {
	t = make(Tags)
	for _, tag := range tags {
		t[tag.Key] = tag.Value
	}
	return t
}

func fieldsMap(fields []*influxdb.Field) (f Fields) {
	f = make(Fields)
	for _, field := range fields {
		f[field.Key] = field.Value
	}
	return f
}
