package queue

type Channel interface {
	Consume(queue string, data interface{}) (ok bool, msg Delivery, err error)
	Publish(queue string, data interface{}) (err error)
	CreateQueue(queue string) (err error)
}

type Delivery interface {
	Ack() error
	Nack(requeue bool) error
}
