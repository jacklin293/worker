package queue

type QueueContainer interface {
	Send(interface{}) (interface{}, error)
	Receive() (interface{}, error)
	// TODO
	// Delete(interface{}) (interface{}, error)
}
