package queue

import (
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
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
	_, err := r.ch.QueueDeclare(queue, true, false, false, false, nil)
	return err
}

func (r *rabbitmq) Consume(queue string, data interface{}) (k bool, err error) {
	msg, ok, err := r.ch.Get(queue, true)

	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	err = yaml.Unmarshal(msg.Body, data)

	if err != nil {
		return false, err
	}

	return true, nil
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
