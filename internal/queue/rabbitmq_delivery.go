package queue

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type rabbitmqDelivery struct {
	msg amqp.Delivery
}

func (rm *rabbitmqDelivery) Ack() error {
	if err := rm.msg.Ack(true); err != nil {
		return errors.Wrap(err, "rabbitmq message ack")
	}

	return nil
}

func (rm *rabbitmqDelivery) Nack(requeue bool) error {
	if err := rm.msg.Nack(true, requeue); err != nil {
		return errors.Wrap(err, "rabbitmq message nack")
	}

	return nil
}
