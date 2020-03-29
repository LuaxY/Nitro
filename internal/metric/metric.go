package metric

import "github.com/influxdata/influxdb-client-go"

type Metric interface {
	Send(metrics ...influxdb.Metric)
}
