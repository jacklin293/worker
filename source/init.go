package source

import "errors"

type Sourcer interface {
	Send([]byte) error
	Receive() ([][]byte, error)
}

func New(c *Config) (s Sourcer, err error) {
	switch c.SourceType {
	case "sqs":
		s = newSQS(c)
	case "go-channel":
		s = newGoChannel(1) // FIXME read size from config
	default:
		err = errors.New("Can't recognise source type - " + c.SourceType)
	}
	return
}
