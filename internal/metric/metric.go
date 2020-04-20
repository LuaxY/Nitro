package metric

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-client-go"
)

type Client interface {
	Add(metric Metric)
	Send(metrics ...*influxdb2.Point)
	Ticker(ctx context.Context, duration time.Duration)
}

type Metric interface {
	Metric() *influxdb2.Point
}

type RowMetric struct {
	Name   string
	Tags   Tags
	Fields Fields
	Time   time.Time
}

func (rm *RowMetric) Metric() *influxdb2.Point {
	return influxdb2.NewPoint(rm.Name, rm.Tags, rm.Fields, rm.Time)
}

type CounterMetric struct {
	RowMetric
	Counter int
}

func (cm *CounterMetric) Inc() {
	cm.Counter++
	cm.Time = time.Now()
}

func (cm *CounterMetric) Metric() *influxdb2.Point {
	return influxdb2.NewPoint(cm.Name, cm.Tags, Fields{"counter": cm.Counter}, cm.Time)
}

type GaugeMetric struct {
	RowMetric
	Gauge int
}

func (gm *GaugeMetric) Set(gauge int) {
	gm.Gauge = gauge
	gm.Time = time.Now()
}

func (gm *GaugeMetric) Metric() *influxdb2.Point {
	return influxdb2.NewPoint(gm.Name, gm.Tags, Fields{"gauge": gm.Gauge}, gm.Time)
}

type DurationMetric struct {
	RowMetric
	Duration time.Duration
}

func (dm *DurationMetric) Set(duration time.Duration) {
	dm.Duration = duration
	dm.Time = time.Now()
}

func (dm *DurationMetric) Metric() *influxdb2.Point {
	return influxdb2.NewPoint(dm.Name, dm.Tags, Fields{"duration": dm.Duration.Milliseconds()}, dm.Time)
}
