package queue

type QueueContainer interface {
	Send(interface{}) (interface{}, error)
	Receive() (interface{}, error)
}
