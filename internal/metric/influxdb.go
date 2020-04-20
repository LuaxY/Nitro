package metric

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-client-go"
	lp "github.com/influxdata/line-protocol"
	log "github.com/sirupsen/logrus"
)

type influx struct {
	client  influxdb2.InfluxDBClient
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
	client := influxdb2.NewClient(config.Addr, config.Token)

	return &influx{client: client, bucket: config.Bucket, org: config.Org}, nil
}

func (i *influx) Add(metric Metric) {
	i.metrics = append(i.metrics, metric)
}

func (i *influx) Send(metrics ...*influxdb2.Point) {
	if err := i.client.WriteApiBlocking(i.org, i.bucket).WritePoint(context.Background(), metrics...); err != nil {
		log.WithError(err).Debug("unable to send metrics:")
		for _, metric := range metrics {
			log.WithFields(log.Fields{
				"name":   metric.Name(),
				"tags":   tagsMap(metric.TagList()),
				"fields": fieldsMap(metric.FieldList()),
			}).Debug("metric not send")
		}
	}

	for _, point := range metrics {
		i.client.WriteApi(i.org, i.bucket).WritePoint(point)
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
			metrics := make([]*influxdb2.Point, len(i.metrics))
			for i, metric := range i.metrics {
				metrics[i] = metric.Metric()
			}
			i.Send(metrics...)
		}
	}
}

func tagsMap(tags []*lp.Tag) (t Tags) {
	t = make(Tags)
	for _, tag := range tags {
		t[tag.Key] = tag.Value
	}
	return t
}

func fieldsMap(fields []*lp.Field) (f Fields) {
	f = make(Fields)
	for _, field := range fields {
		f[field.Key] = field.Value
	}
	return f
}
