package queue

import (
	"github.com/streadway/amqp"
)

type rabbitmq struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbitMQ(url string) (Channel, error) {
	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &rabbitmq{conn: conn, ch: ch}, nil
}

func (r *rabbitmq) CreateQueue(queue string) error {
	_, err := r.ch.QueueDeclare(queue, false, false, false, false, nil)
	return err
}

func (r *rabbitmq) Consume(queue string) (data []byte, ok bool, err error) {
	msg, ok, err := r.ch.Get(queue, true)

	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	return msg.Body, true, nil
}

func (r *rabbitmq) Publish(queue string, data []byte) (err error) {
	return r.ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType: "text/yaml",
		Body:        data,
	})
}
