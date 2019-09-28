package source

type Sourcer interface {
	Send(interface{}) (interface{}, error)
	Receive() (interface{}, error)
}
