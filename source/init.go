package source

type Sourcer interface {
	Send([]byte) error
	Receive() ([][]byte, error)
}
