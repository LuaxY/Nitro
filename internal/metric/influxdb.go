package metric

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-client-go"
	log "github.com/sirupsen/logrus"
)

type influx struct {
	client *influxdb.Client
	bucket string
	org    string
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

func RowMetric(name string, tags Tags, fields Fields) influxdb.Metric {
	return influxdb.NewRowMetric(fields, name, tags, time.Now())
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
