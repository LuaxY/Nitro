package queue

type Channel interface {
	Consume(queue string, data interface{}) (ok bool, err error)
	Publish(queue string, data interface{}) (err error)
	CreateQueue(queue string) (err error)
}
