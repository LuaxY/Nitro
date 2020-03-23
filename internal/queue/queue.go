package queue

type Channel interface {
	Consume(queue string) (data []byte, ok bool, err error)
	Publish(queue string, data []byte) (err error)
	CreateQueue(queue string) (err error)
}
