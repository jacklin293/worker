package source

type Sourcer interface {
	Send([]byte) error
	Receive() ([][]byte, error)
}

// Source types that are supported
var supportedSourceTypes = []string{"sqs", "go_channel"}
