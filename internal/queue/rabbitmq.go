package queue

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

type rabbitmq struct {
	ctx        context.Context
	url        string
	connClosed chan *amqp.Error
	chClosed   chan *amqp.Error
	conn       *amqp.Connection
	ch         *amqp.Channel
	queues     []string
}

func NewRabbitMQ(ctx context.Context, url string) (Channel, error) {
	client := &rabbitmq{ctx: ctx, url: url}

	client.connect()
	go client.reconnect()

	return client, nil
}

func (r *rabbitmq) connect() {
	var err error

	for {
		r.conn, err = amqp.Dial(r.url)

		if err != nil {
			log.WithError(err).Warn("unable to connect to rabbitmq, retrying is 1 sec...")
			time.Sleep(1 * time.Second)
			continue
		}

		log.Debugf("connected to rabbitmq: %s", r.url)
		r.connClosed = make(chan *amqp.Error)
		r.conn.NotifyClose(r.connClosed)
		r.openChanel()
		return
	}
}

func (r *rabbitmq) openChanel() {
	var err error

	for {
		r.ch, err = r.conn.Channel()

		if err != nil {
			log.WithError(err).Warn("unable to open rabbitmq channel, retrying is 1 sec...")
			time.Sleep(1 * time.Second)
			continue
		}

		log.Debug("rabbitmq channel open")
		r.chClosed = make(chan *amqp.Error)
		r.ch.NotifyClose(r.chClosed)

		for _, queue := range r.queues {
			if _, err := r.ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
				log.WithError(err).Warnf("unable to redeclare queue '%s'", queue)
			}
		}

		return
	}
}

func (r *rabbitmq) reconnect() {
	for {
		select {
		case <-r.ctx.Done():
			_ = r.conn.Close()
			return
		case err := <-r.connClosed:
			log.WithError(err).Warn("rabbitmq connection closed, reconnect")
			r.connect()
		case err := <-r.chClosed:
			log.WithError(err).Warn("rabbitmq channel closed, reopen")
			r.openChanel()
		}
	}
}

func (r *rabbitmq) CreateQueue(queue string) error {
	r.queues = append(r.queues, queue)
	_, err := r.ch.QueueDeclare(queue, true, false, false, false, nil)
	return err
}

func (r *rabbitmq) Consume(queue string, data interface{}) (k bool, msg Delivery, err error) {
	message, ok, err := r.ch.Get(queue, false)

	if err != nil {
		return false, nil, err
	}

	if !ok {
		return false, nil, nil
	}

	err = yaml.Unmarshal(message.Body, data)

	if err != nil {
		return false, nil, err
	}

	return true, &rabbitmqDelivery{msg: message}, nil
}

func (r *rabbitmq) Publish(queue string, data interface{}) (err error) {
	body, err := yaml.Marshal(data)

	if err != nil {
		return err
	}

	return r.ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType: "text/yaml",
		Body:        body,
	})
}
