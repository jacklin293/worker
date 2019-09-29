package queue

type Queuer interface {
	Send(interface{}) (interface{}, error)
	Receive() (interface{}, error)
}
