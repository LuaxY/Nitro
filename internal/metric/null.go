package metric

import (
	"context"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
)

type Null struct {
}

func (n *Null) Add(metric Metric) {

}

func (n *Null) Send(metrics ...*influxdb2.Point) {

}

func (n *Null) Ticker(ctx context.Context, duration time.Duration) {

}
